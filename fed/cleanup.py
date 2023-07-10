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
from collections import deque

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
        self._sending_obj_refs_q = deque()
        self._check_send_thread = None
        self._monitor_thread = None

    def start(self, exit_when_failure_sending=False):
        self._exit_when_failure_sending = exit_when_failure_sending

        def __check_func():
            self._check_sending_objs()

        self._check_send_thread = threading.Thread(target=__check_func)
        self._check_send_thread.start()
        logger.info('Start check sending thread.')

        def _main_thread_monitor():
            main_thread = threading.main_thread()
            main_thread.join()
            self._notify_to_exit()

        self._monitor_thread = threading.Thread(target=_main_thread_monitor)
        self._monitor_thread.start()
        logger.info('Start check sending monitor thread.')

    def graceful_stop(self):
        assert self._check_send_thread is not None
        self._notify_to_exit()
        self._check_send_thread.join()

    def _notify_to_exit(self):
        # Sending the termination signal
        self.push_to_sending(True)
        logger.info('Notify check sending thread to exit.')

    def push_to_sending(self, obj_ref: ray.ObjectRef):
        self._sending_obj_refs_q.append(obj_ref)

    def _check_sending_objs(self):
        def _signal_exit():
            os.kill(os.getpid(), signal.SIGTERM)

        assert self._sending_obj_refs_q is not None

        while True:
            try:
                obj_ref = self._sending_obj_refs_q.popleft()
            except IndexError:
                time.sleep(0.1)
                continue
            if isinstance(obj_ref, bool):
                break
            try:
                res = ray.get(obj_ref)
            except Exception as e:
                logger.warn(f'Failed to send {obj_ref} with error: {e}')
                res = False
            if not res and self._exit_when_failure_sending:
                logger.warn('Signal self to exit.')
                _signal_exit()
                break

        logger.info('Check sending thread was exited.')
