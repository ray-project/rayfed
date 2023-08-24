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

STOP_SYMBOL = False

class MessageQueue:
    def __init__(self, msg_handler, failure_handler=None, name=''):
        assert callable(msg_handler), "msg_handler must be a callable function"

        self._queue = deque()
        self._msg_handler = msg_handler
        self._failure_handler = failure_handler
        self._thread = None
        self._name = name

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
            logger.debug(f"Starting new thread[{self._name}] for message polling.")
            self._queue = deque()
            self._thread = threading.Thread(target=_loop)
            self._thread.start()

    def push(self, message):
        self._queue.append(message)

    def notify_to_exit(self):
        logger.info(f"Notify message polling thread[{self._name}] to exit.")
        self.push(STOP_SYMBOL)

    def stop(self, graceful=True):
        """
        Stop the message queue.

        If the graceful flag is set to True, it will wait for the current message to be processed before stopping.
        If the graceful flag is set to False, it will stop the queue immediately.

        Args:
            graceful (bool): A flag indicating whether to stop the queue gracefully or not. Default is True.
                If True: insert the STOP_SYMBOL at the end of the queue and wait for it to be processed, which
                    will break the for-loop
                If False: forcelly kill the for-loop sub-thread
        """
        # TODO: 这行代码是毒瘤，导致 stop 本身预期是同步调用，一定能把对应子线程 join 掉，但实际上不会。
        if threading.current_thread() == self._thread:
            logger.warning(f"Can't stop the message queue in the message"
                           f"polling thread[{self._name}], ignore it, this."
                           f"could bring unknown timing problem.")
            return

        if graceful:
            if self.is_started():
                self.notify_to_exit()
                self._thread.join()
        else:
            if self._thread is not None:
                self._thread._stop()
        logger.info(f"The message polling thread[{self._name}] was exited.")

    def is_started(self):
        return self._thread is not None and self._thread.is_alive()
