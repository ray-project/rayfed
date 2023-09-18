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

import threading
from collections import deque
import time
import logging


logger = logging.getLogger(__name__)

# NOTE(NKcqx): The symbol to let the polling thread inside message queue to stop.
# Because in python, the recommended way to stop a sub-thread is to set a flag
# that checked by the sub-thread itself.(see https://stackoverflow.com/a/325528).
STOP_SYMBOL = False


class MessageQueueManager:
    def __init__(self, msg_handler, failure_handler=None, thread_name=''):
        assert callable(msg_handler), "msg_handler must be a callable function"
        # `deque()` is thread safe on `popleft` and `append` operations.
        # See https://docs.python.org/3/library/collections.html#deque-objects
        self._queue = deque()
        self._msg_handler = msg_handler
        self._failure_handler = failure_handler
        self._thread = None
        # Assign a name to the thread to better distinguish it from all threads.
        self._thread_name = thread_name

    def start(self):
        def _loop():
            while True:
                try:
                    message = self._queue.popleft()
                except IndexError:
                    time.sleep(0.1)
                    continue

                if message == STOP_SYMBOL:
                    break
                res = self._msg_handler(message)
                if not res:
                    break

        if self._thread is None or not self._thread.is_alive():
            logger.debug(
                f"Starting new thread[{self._thread_name}] for message polling.")
            self._queue = deque()
            self._thread = threading.Thread(target=_loop, name=self._thread_name)
            self._thread.start()

    def append(self, message):
        self._queue.append(message)

    def notify_to_exit(self):
        logger.info(f"Notify message polling thread[{self._thread_name}] to exit.")
        self.append(STOP_SYMBOL)

    def stop(self):
        """
        Stop the message queue.

        Args:
            graceful (bool): A flag indicating whether to stop the queue
                    gracefully or not. Default is True.
                If True: insert the STOP_SYMBOL at the end of the queue
                    and wait for it to be processed, which will break the for-loop;
                If False: forcelly kill the for-loop sub-thread.
        """
        if threading.current_thread() == self._thread:
            logger.error(f"Can't stop the message queue in the message "
                         f"polling thread[{self._thread_name}]. Ignore it as this"
                         f"could bring unknown time sequence problems.")
            raise RuntimeError("Thread can't kill itself")

        # TODO(NKcqx): Force kill sub-thread by calling `._stop()` will
        # encounter AssertionError because sub-thread's lock is not released.
        # Therefore, currently, not support forcelly kill thread
        if self.is_started():
            logger.debug(f"Gracefully killing thread[{self._thread_name}].")
            self.notify_to_exit()
            self._thread.join()

        logger.info(f"The message polling thread[{self._thread_name}] was exited.")

    def is_started(self):
        return self._thread is not None and self._thread.is_alive()
