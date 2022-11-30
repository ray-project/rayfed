# import queue
import logging
import threading
import time
from collections import deque

import ray

logger = logging.getLogger(__name__)
_sending_obj_refs_q = deque()

_check_send_thread = None


def _check_sending_objs():
    global _sending_obj_refs_q
    while True:
        try:
            obj_ref = _sending_obj_refs_q.popleft()
        except IndexError:
            time.sleep(0.5)
            continue
        if isinstance(obj_ref, bool):
            break
        try:
            ray.get(obj_ref)
        except Exception as e:
            logger.warn(f'Failed to send {obj_ref} with error: {e}')

    logger.info('Check sending thread was exited.')
    global _check_send_thread
    _check_send_thread = None


def _monitor_thread():
    main_thread = threading.main_thread()
    main_thread.join()
    notify_to_exit()


_monitor_thread = None


def _start_check_sending():
    global _check_send_thread
    if not _check_send_thread:
        _check_send_thread = threading.Thread(target=_check_sending_objs)
        _check_send_thread.start()
        logger.info('Start check sending thread.')

        global _monitor_thread
        if not _monitor_thread:
            _monitor_thread = threading.Thread(target=_monitor_thread)
            _monitor_thread.start()
            logger.info('Start check sending monitor thread.')


def push_to_sending(obj_ref: ray.ObjectRef):
    _start_check_sending()
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(obj_ref)


def notify_to_exit():
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(True)
    logger.info('Notify check sending thread to exit.')


def wait_sending():
    global _check_send_thread
    if _check_send_thread:
        notify_to_exit()
        _check_send_thread.join()
