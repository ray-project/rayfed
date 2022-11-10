# import queue
import ray
import threading
from collections import deque
import time

_sending_obj_refs_q = deque()
_exit_flag = False

def _check_sending_objs():
    global _sending_obj_refs_q
    while not _exit_flag:
        try:
            obj_ref = _sending_obj_refs_q.popleft()
        except IndexError:
            time.sleep(0.5)
            continue
        ray.get(obj_ref)
    
    import os
    print(f'{os.getpid()} check sending thread exit.')


_check_send_thread = threading.Thread(target=_check_sending_objs)
_check_send_thread_started = False


def _monitor_thread():
    main_thread = threading.main_thread()
    main_thread.join()
    import os
    print(f'{os.getpid()} will exit and notify check sending thread to exit.')
    global _exit_flag
    _exit_flag = True

_monitor = threading.Thread(target=_monitor_thread)


def _start_check_sending():
    global _check_send_thread
    global _check_send_thread_started
    if not _check_send_thread_started:
        _check_send_thread.start()
        import os
        print(f'{os.getpid()} start check sending thread.')
        global _monitor
        _monitor.start()
        print(f'{os.getpid()} start check sending monitor thread.')
        _check_send_thread_started = True

def push_to_sending(obj_ref: ray.ObjectRef):
    _start_check_sending()
    global _sending_obj_refs_q
    _sending_obj_refs_q.append(obj_ref)
