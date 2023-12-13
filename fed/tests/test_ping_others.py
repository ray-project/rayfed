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
import time

import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils
from fed.proxy.barriers import ping_others

addresses = {
    'alice': '127.0.0.1:11012',
    'bob': '127.0.0.1:11011',
}


def test_ping_non_started_party():
    def run(party):
        compatible_utils.init_ray(address='local')
        fed.init(addresses=addresses, party=party)
        if party == 'alice':
            with pytest.raises(RuntimeError):
                ping_others(addresses, party, 5)

        fed.shutdown()
        ray.shutdown()

    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_alice.start()
    p_alice.join()


def test_ping_started_party():
    def run(party):
        compatible_utils.init_ray(address='local')
        fed.init(addresses=addresses, party=party)
        if party == 'alice':
            ping_success = ping_others(addresses, party, 5)
            assert ping_success is True
        else:
            # Wait for alice to ping, otherwise, bob may
            # exit before alice when started first.
            time.sleep(10)
        fed.shutdown()
        ray.shutdown()

    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
