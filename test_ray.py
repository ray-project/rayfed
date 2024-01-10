
# test_ray.py

import os
import ray
import signal

def signal_handler(sig, frame):
    if sig == signal.SIGTERM.value:
        ray.shutdown()
        os._exit(0)

signal.signal(signal.SIGTERM, signal_handler)

ray.init()


import time
print(f"========sleeping...")
time.sleep(86400)

