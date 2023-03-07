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
import fed

import signal

import sys


def signal_handler(sig, frame):
    if sig == signal.SIGTERM.value:
        fed.shutdown()
        sys.exit(0)


@fed.remote
def f():
    return 100


@fed.remote
class My:
    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value


def run(party, is_inner_party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    retry_policy = {
        "maxAttempts": 2,
        "initialBackoff": "1s",
        "maxBackoff": "1s",
        "backoffMultiplier": 1,
        "retryableStatusCodes": ["UNAVAILABLE"],
    }
    fed.init(
        address='local',
        cluster=cluster,
        party=party,
        logging_level='debug',
        cross_silo_grpc_retry_policy=retry_policy,
        exit_on_failure_cross_silo_sending=True,
    )

    o = f.party("alice").remote()
    My.party("bob").remote(o)
    import time

    # Wait for SIGTERM as failure on sending.
    time.sleep(86400)


def test_exit_when_failure_on_sending():
    signal.signal(signal.SIGTERM, signal_handler)
    p_alice = multiprocessing.Process(target=run, args=('alice', True))
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
