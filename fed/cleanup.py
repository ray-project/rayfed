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
import time
# from collections import deque
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
            lambda msg: self._process_data_message(msg))

        self._sending_error_q = MessageQueue(
            lambda msg: self._process_error_message(msg))

        self._monitor_thread = None

    def start(self, exit_on_sending_failure=False):
        self._exit_on_sending_failure = exit_on_sending_failure

        self._sending_data_q.start()
        logger.info('Start check sending thread.')
        self._sending_error_q.start()
        logger.info('Start check error sending thread.')

        def _main_thread_monitor():
            main_thread = threading.main_thread()
            main_thread.join()
            self.graceful_stop()

        self._monitor_thread = threading.Thread(target=_main_thread_monitor)
        self._monitor_thread.start()
        logger.info('Start check sending monitor thread.')

    def graceful_stop(self):
        # NOTE(Nkcqx): MUST firstly stop the data queue, because it
        # may still throw errors during the termination which need to
        # be sent to the error queue.
        self._sending_data_q.stop()
        self._sending_error_q.stop()

    def push_to_sending(self,
                        obj_ref: ray.ObjectRef,
                        upstream_seq_id: int,
                        downstream_seq_id: int,
                        is_error=False):
        msg_pack = (obj_ref, upstream_seq_id, downstream_seq_id)
        if (is_error):
            self._sending_error_q.push(msg_pack)
        else:
            self._sending_data_q.push(msg_pack)

    def _signal_exit(self):
        """
        Exit the current process immediately.
        Since the compuation is interrupt, no data need to be sent to
        other parties, therefore the data queue can stop forcelly;
        But the errors should be sent to other parties as many as possible
        so that they can exit in time too.
        """
        # No need to send any data to other parties
        self._sending_data_q.stop(graceful=False)
        # Still 
        self._sending_error_q.stop()
        os.kill(os.getpid(), signal.SIGTERM)

    def _process_data_message(self, message):
        """
        This is the handler function that is called in the message queue's
        processing sub-thread.
        """
        obj_ref, upstream_seq_id, downstream_seq_id = message
        try:
            res = ray.get(obj_ref)
        except Exception as e:
            logger.warn(f'Failed to send {obj_ref} with error: {e},'
                        f'upstream_seq_id: {upstream_seq_id},'
                        f'downstream_seq_id: {downstream_seq_id}.')
            if (isinstance(e, RayError) and e.cause()):
                # TODO: Send error message
                pass
            res = False

        if not res and self._exit_on_sending_failure:
            # NOTE(NKcqx): this will exit the data sending thread and
            # the error sending thread. However, the former will IGNORE
            # the stop command because this function is called inside
            # the data sending thread but it can't kill itself. The
            # data sending thread is exiting by the return value.
            self._signal_exit()
            # This will notify the queue to break the for-loop and
            # exit the thread.
            return False
        return True


    def _process_error_message(self, error_msg):
        error_ref, upstream_seq_id, downstream_seq_id = error_msg
        try:
            res = ray.get(error_ref)
        except Exception as e:
            logger.warn(f"Failed to brodcast error {error_ref}, "
                        f"this may cause other parties hang since "
                        f"they can't sense this error and wait forever.")
            return False