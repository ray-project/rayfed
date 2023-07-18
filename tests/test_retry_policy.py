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
import fed._private.compatible_utils as compatible_utils
import ray

from fed.config import GrpcCrossSiloMsgConfig


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
    compatible_utils.init_ray(address='local')
    cluster = {
        'alice': {'address': '127.0.0.1:11012'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    retry_policy = {
        "maxAttempts": 4,
        "initialBackoff": "5s",
        "maxBackoff": "5s",
        "backoffMultiplier": 1,
        "retryableStatusCodes": ["UNAVAILABLE"],
    }
    fed.init(
        cluster=cluster,
        party=party,
        global_cross_silo_comm_config=GrpcCrossSiloMsgConfig(
            grpc_retry_policy=retry_policy
        )
    )

    o = f.party("alice").remote()
    actor_location = "alice" if is_inner_party else "bob"
    my = My.party(actor_location).remote(o)
    val = my.get_value.remote()
    result = fed.get(val)
    assert result == 100
    assert fed.get(o) == 100
    fed.shutdown()
    ray.shutdown()


def test_listen_addr():
    p_alice = multiprocessing.Process(target=run, args=('alice', True))
    p_bob = multiprocessing.Process(target=run, args=('bob', True))
    p_alice.start()

    import time

    time.sleep(10)
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
