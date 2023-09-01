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
from fed._private.queue import MessageQueue
from ray.exceptions import RayError

import ray

logger = logging.getLogger(__name__)


class CleanupManager:
    """
    This class is used to manage the related works when the fed driver exiting.
    It monitors whether the main thread is broken and it needs wait until all sending
    objects get repsonsed.

    The main logic path is:
        A. If `fed.shutdown()` is invoked in the main thread and every thing works well,
        the `graceful_stop()` will be invoked as well and the checking thread will be
        notifiled to exit gracefully.

        B. If the main thread are broken before sending the notification flag to the
        sending thread, the monitor thread will detect that and it joins until the main
        thread exited, then notifys the checking thread.
    """

    def __init__(self) -> None:
        # `deque()` is thread safe on `popleft` and `append` operations.
        # See https://docs.python.org/3/library/collections.html#deque-objects
        self._sending_data_q = MessageQueue(
            lambda msg: self._process_data_message(msg),
            name='DataQueue')

        self._sending_error_q = MessageQueue(
            lambda msg: self._process_error_message(msg),
            name="ErrorQueue")

        self._monitor_thread = None

    def start(self, exit_on_sending_failure=False):
        self._exit_on_sending_failure = exit_on_sending_failure

        self._sending_data_q.start()
        logger.debug('Start check sending thread.')
        self._sending_error_q.start()
        logger.debug('Start check error sending thread.')

        def _main_thread_monitor():
            main_thread = threading.main_thread()
            main_thread.join()
            self.graceful_stop()

        self._monitor_thread = threading.Thread(target=_main_thread_monitor)
        self._monitor_thread.start()
        logger.info('Start check sending monitor thread.')

    def graceful_stop(self):
        # NOTE(NKcqx): MUST firstly stop the data queue, because it
        # may still throw errors during the termination which need to
        # be sent to the error queue.
        self._sending_data_q.stop()
        self._sending_error_q.stop()

    def push_to_sending(self,
                        obj_ref: ray.ObjectRef,
                        dest_party: str = None,
                        upstream_seq_id: int = -1,
                        downstream_seq_id: int = -1,
                        is_error: bool = False):
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
        if (is_error):
            self._sending_error_q.push(msg_pack)
        else:
            self._sending_data_q.push(msg_pack)

    def _signal_exit(self):
        """
        Exit the current process immediately. The signal will be captured
        in main thread where the `stop` will be called.
        """
        logger.debug("Signal SIGINT to exit.")
        os.kill(os.getpid(), signal.SIGINT)

    def _process_data_message(self, message):
        """
        This is the handler function for message queue and will be called
        in the sub-thread.
        """
        obj_ref, dest_party, upstream_seq_id, downstream_seq_id = message
        try:
            res = ray.get(obj_ref)
        except Exception as e:
            logger.warn(f'Failed to send {obj_ref} to {dest_party}, error: {e},'
                        f'upstream_seq_id: {upstream_seq_id}, '
                        f'downstream_seq_id: {downstream_seq_id}.')
            if (isinstance(e, RayError)):
                logger.info(f"Sending error {e.cause} to {dest_party}.")
                from fed.proxy.barriers import send
                # TODO(NKcqx): Maybe broadcast to all parties?
                send(dest_party, e.cause, upstream_seq_id, downstream_seq_id, True)

            res = False

        if not res and self._exit_on_sending_failure:
            # NOTE(NKcqx): Send signal to main thread so that it can
            # do some cleaning, e.g. kill the error sending thread.
            self._signal_exit()
            # Return False to exit the loop in sub-thread. Note that
            # the above signal will also make the main thread to kill
            # the sub-thread eventually.
            return False
        return True

    def _process_error_message(self, error_msg):
        error_ref, dest_party, upstream_seq_id, downstream_seq_id = error_msg
        try:
            res = ray.get(error_ref)
            logger.debug(f"Sending error got response: {res}.")
        except Exception:
            res = False

        if not res:
            logger.warning(f"Failed to send error {error_ref} to {dest_party}, "
                           f"upstream_seq_id: {upstream_seq_id} "
                           f"downstream_seq_id: {downstream_seq_id}. "
                           "In this case, other parties won't sense "
                           "this error and may cause unknown behaviour.")
        # Return True so that one error object won't affect others to send
        return True
