# import queue
import logging
import threading
import time
from collections import deque

import ray

logger = logging.getLogger(__name__)
_sending_obj_refs_q = deque()


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
            print(f'send {obj_ref} failed, \n {e}')

    import os

    logger.info(f'{os.getpid()} check sending thread exit.')


_check_send_thread = threading.Thread(target=_check_sending_objs)
_check_send_thread_started = False


def _monitor_thread():
    main_thread = threading.main_thread()
    main_thread.join()
    import os

    logger.info(f'{os.getpid()} will exit and notify check sending thread to exit.')
    notify_to_exit()


_monitor = threading.Thread(target=_monitor_thread)


def _start_check_sending():
    global _check_send_thread
    global _check_send_thread_started
    if not _check_send_thread_started:
        _check_send_thread.start()
        import os

        logger.info(f'{os.getpid()} start check sending thread.')
        global _monitor
        _monitor.start()
        logger.info(f'{os.getpid()} start check sending monitor thread.')
        _check_send_thread_started = True


def push_to_sending(obj_ref: ray.ObjectRef):
    _start_check_sending()
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(obj_ref)


def notify_to_exit():
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(True)


def wait_sending():
    global _check_send_thread_started
    if _check_send_thread_started:
        notify_to_exit()
        _check_send_thread.join()
