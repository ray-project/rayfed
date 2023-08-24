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

import multiprocessing

import pytest
import ray
import fed
import fed._private.compatible_utils as compatible_utils
import signal
import os
import sys


def signal_handler(sig, frame):
    if sig == signal.SIGTERM.value:
        fed.shutdown()
        ray.shutdown()
        os._exit(0)


class MyError(Exception):
    def __init__(self, message):
        super().__init__(message)


@fed.remote
def error_func():
    raise MyError("Test Error")


@fed.remote
class My:
    def __init__(self) -> None:
        pass

    def error_func(self):
        raise MyError("Test Error")


def run(party):
    signal.signal(signal.SIGTERM, signal_handler)

    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }

    fed.init(
        addresses=addresses,
        party=party,
        logging_level='debug',
        config={
            'cross_silo_comm': {
                'exit_on_sending_failure': False,
                'timeout_ms': 20 * 1000,
            },
        },
    )

    # Both party should catch the error and in the
    # exact type.
    o = error_func.party("alice").remote()
    with pytest.raises(Exception):
        fed.get(o)

    actor = My.party("alice").remote()
    with pytest.raises(Exception):
        fed.get(actor.error_func.remote())


def test_cross_silo_error():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0
    assert p_bob.exitcode == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
