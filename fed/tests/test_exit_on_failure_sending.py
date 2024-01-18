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
import sys

import pytest

import fed
import fed._private.compatible_utils as compatible_utils


@fed.remote
def f():
    raise Exception('By design.')


@fed.remote
class My:
    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value


def run(party: str, q: multiprocessing.Queue):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:21321',
        'bob': '127.0.0.1:21322',
    }
    retry_policy = {
        "maxAttempts": 2,
        "initialBackoff": "1s",
        "maxBackoff": "1s",
        "backoffMultiplier": 1,
        "retryableStatusCodes": ["UNAVAILABLE"],
    }

    def failure_handler(error):
        q.put('failure handler')

    fed.init(
        addresses=addresses,
        party=party,
        logging_level='debug',
        config={
            'cross_silo_comm': {
                'grpc_retry_policy': retry_policy,
                'exit_on_sending_failure': True,
                'timeout_ms': 20 * 1000,
            },
        },
        sending_failure_handler=failure_handler,
    )
    o = f.party("alice").remote()
    My.party("bob").remote(o)

    import time

    # Wait a long time.
    # If the test takes effect, the main loop here will be broken.
    time.sleep(86400)


def test_exit_when_failure_on_sending():
    q = multiprocessing.Queue()
    p_alice = multiprocessing.Process(target=run, args=('alice', q))
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 1
    assert q.get() == 'failure handler'


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
