# Copyright 2023 The RayFed Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import signal
import threading

import ray
from ray.exceptions import RayError

from fed._private.message_queue import MessageQueueManager
from fed.exceptions import FedRemoteError

logger = logging.getLogger(__name__)


class CleanupManager:
    """
    This class is used to manage the related works when the fed driver exiting.
    It monitors whether the main thread is broken and it needs wait until all sending
    objects get repsonsed.

    The main logic path is:
        A. If `fed.shutdown()` is invoked in the main thread and every thing works well,
        the `stop()` will be invoked as well and the checking thread will be
        notified to exit gracefully.

        B. If the main thread are broken before sending the stop flag to the sending
        thread, the monitor thread will detect that and then notifys the checking
        thread.
    """

    def __init__(self, current_party, acquire_shutdown_flag) -> None:
        self._sending_data_q = MessageQueueManager(
            lambda msg: self._process_data_sending_task_return(msg),
            thread_name="DataSendingQueueThread",
        )

        self._sending_error_q = MessageQueueManager(
            lambda msg: self._process_error_sending_task_return(msg),
            thread_name="ErrorSendingQueueThread",
        )

        self._monitor_thread = None

        self._current_party = current_party
        self._acquire_shutdown_flag = acquire_shutdown_flag
        self._last_sending_error = None

    def start(self, exit_on_sending_failure=False, expose_error_trace=False):
        self._exit_on_sending_failure = exit_on_sending_failure
        self._expose_error_trace = expose_error_trace

        self._sending_data_q.start()
        logger.debug("Start check sending thread.")
        self._sending_error_q.start()
        logger.debug("Start check error sending thread.")

    def stop(self, wait_for_sending=False):
        # NOTE(NKcqx): MUST firstly stop the data queue, because it
        # may still throw errors during the termination which need to
        # be sent to the error queue.
        self._sending_data_q.stop(wait_for_sending=wait_for_sending)
        self._sending_error_q.stop(wait_for_sending=wait_for_sending)

    def push_to_sending(
        self,
        obj_ref: ray.ObjectRef,
        dest_party: str = None,
        upstream_seq_id: int = -1,
        downstream_seq_id: int = -1,
        is_error: bool = False,
    ):
        """
        Push the sending remote task's return value, i.e. `obj_ref` to
        the corresponding message queue.

        Args:
            obj_ref: The return value of the send remote task.
            dest_party: Destination
            upstream_seq_id: (Optional) This is unneccessary when sending
                normal data message because it was already sent to the target
                party. However, if the computation is corrupted, an error object
                will be created and sent with the same seq_id to replace the
                original data object. This argument is used to send the error.
            downstream_seq_id: (Optional) Same as `upstream_seq_id`.
            is_error: (Optional) Whether the obj_ref represent an error object or not.
                Default to False. If True, the obj_ref will be sent to the error
                queue instead.
        """
        msg_pack = (obj_ref, dest_party, upstream_seq_id, downstream_seq_id)
        if is_error:
            self._sending_error_q.append(msg_pack)
        else:
            self._sending_data_q.append(msg_pack)

    def get_last_sending_error(self):
        return self._last_sending_error

    def _signal_exit(self):
        """
        Exit the current process immediately. The signal will be captured
        in main thread where the `stop` will be called.
        """
        # NOTE(NKcqx): The signal is implemented by the error mechanism,
        # a `KeyboardInterrupt` will be raised after sending the signal,
        # and OS will hold
        # the process's original context and change to the error handler context.
        # States that the original context hold, including `threading.lock`,
        # will not be released, acquiring the same lock in signal handler
        # will cause dead lock. In order to ensure executing `shutdown` exactly
        # once and avoid dead lock, the lock must be checked before sending
        # signals.
        if self._acquire_shutdown_flag():
            logger.warn("Signal SIGINT to exit.")
            os.kill(os.getpid(), signal.SIGINT)

    def _process_data_sending_task_return(self, message):
        """
        This is the message handler function used in message queue for
        processing each element. The element is putted from `barriers.send`
        and is a quadruple of <obj_ref, dest_party, upstream_seq_id, downstream_seq_id>.

        The `obj_ref` is the task return value of `sender_proxy.send.remote`.
        It `obj_ref` needs `ray.get` to trigger the execution of the corresponding
        task, which is the main functionality of this handler function.

        If any exception occurs during `ray.get`, it indicates that the data cannot
        be sent to other party normally. In order to notify the other party the current
        situation and prevent it from hanging, a RemoteError object will be constructed
        to replace the origin data object, and try to send it again.

        Return:
            bool: True, means the processing is success. The message queue will keep
                    polling.
                  False, make the message queue stop polling.
        """
        obj_ref, dest_party, upstream_seq_id, downstream_seq_id = message
        try:
            res = ray.get(obj_ref)
        except Exception as e:
            logger.warn(
                f"Failed to send {obj_ref} to {dest_party}, error: {e},"
                f"upstream_seq_id: {upstream_seq_id}, "
                f"downstream_seq_id: {downstream_seq_id}."
            )
            self._last_sending_error = e
            if isinstance(e, RayError):
                logger.info(f"Sending error {e.cause} to {dest_party}.")
                from fed.proxy.barriers import send

                # TODO(NKcqx): Cascade broadcast to all parties
                error_trace = e.cause if self._expose_error_trace else None
                send(
                    dest_party,
                    FedRemoteError(self._current_party, error_trace),
                    upstream_seq_id,
                    downstream_seq_id,
                    True,
                )

            res = False

        if not res and self._exit_on_sending_failure:
            # NOTE(NKcqx): Send signal to main thread so that it can
            # do some cleaning, e.g. kill the error sending thread.
            self._signal_exit()
            # Return False to exit the loop in sub-thread. Note that
            # the above signal will also make the main thread to kill
            # the sub-thread eventually by pushing a stop flag.
            return False
        return True

    def _process_error_sending_task_return(self, error_msg):
        error_ref, dest_party, upstream_seq_id, downstream_seq_id = error_msg
        try:
            res = ray.get(error_ref)
            logger.debug(f"Sending error got response: {res}.")
        except Exception:
            res = False

        if not res:
            logger.warning(
                f"Failed to send error {error_ref} to {dest_party}, "
                f"upstream_seq_id: {upstream_seq_id} "
                f"downstream_seq_id: {downstream_seq_id}. "
                "In this case, other parties won't sense "
                "this error and may cause unknown behaviour."
            )
        # Return True so that remaining error objects can be sent
        return True
