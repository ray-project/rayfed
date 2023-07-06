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

_sending_obj_refs_q = None

_check_send_thread = None

_EXIT_ON_FAILURE_SENDING = False

_lock_on_sending_q = threading.Lock()

_lock_on_send_thread = threading.Lock()

class LockContext:
    def __init__(self, lock):
        self.lock = lock
    
    def __enter__(self):
        self.lock.acquire()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()


def set_exit_on_failure_sending(exit_when_failure_sending: bool):
    global _EXIT_ON_FAILURE_SENDING
    _EXIT_ON_FAILURE_SENDING = exit_when_failure_sending


def get_exit_when_failure_sending():
    global _EXIT_ON_FAILURE_SENDING
    return _EXIT_ON_FAILURE_SENDING


def _check_sending_objs():
    def _signal_exit():
        os.kill(os.getpid(), signal.SIGTERM)

    global _sending_obj_refs_q
    if not _sending_obj_refs_q:
        _sending_obj_refs_q = deque()

    while True:
        try:
            obj_ref = _sending_obj_refs_q.popleft()
        except IndexError:
            time.sleep(0.5)
            continue
        if isinstance(obj_ref, bool):
            break
        try:
            res = ray.get(obj_ref)
        except Exception as e:
            logger.warn(f'Failed to send {obj_ref} with error: {e}')
            res = False
        if not res and get_exit_when_failure_sending():
            logger.warn('Signal self to exit.')
            _signal_exit()
            break

    logger.info('Check sending thread was exited.')

    logger.info('Clearing sending queue.')
    _sending_obj_refs_q = None


def _main_thread_monitor():
    main_thread = threading.main_thread()
    main_thread.join()
    notify_to_exit()


_monitor_thread = None


def _start_check_sending():
    global _sending_obj_refs_q
    if not _sending_obj_refs_q:
        _sending_obj_refs_q = deque()

    need_initialize = False
    global _lock_on_send_thread
    with LockContext(_lock_on_send_thread):
        need_initialize = True
        global _check_send_thread
        if not _check_send_thread:
            _check_send_thread = threading.Thread(target=_check_sending_objs)
            _check_send_thread.start()
            logger.info('Start check sending thread.')

    if need_initialize:
        global _monitor_thread
        if not _monitor_thread:
            _monitor_thread = threading.Thread(target=_main_thread_monitor)
            _monitor_thread.start()
            logger.info('Start check sending monitor thread.')


def push_to_sending(obj_ref: ray.ObjectRef):
    _start_check_sending()
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(obj_ref)


def notify_to_exit():
    # Sending the termination signal
    push_to_sending(True)
    logger.info('Notify check sending thread to exit.')


def wait_sending():
    global _lock_on_send_thread
    with LockContext(_lock_on_send_thread):
        global _check_send_thread
        if _check_send_thread:
            notify_to_exit()
            _check_send_thread.join()
            _check_send_thread = None
