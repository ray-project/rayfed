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
import os

import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils


@fed.remote
class My:
    def __init__(self) -> None:
        self._val = 0

    def incr(self, delta):
        self._val += delta
        return self._val


@fed.remote
def add(x, y):
    return x + y


def _run(party: str):
    compatible_utils.init_ray(address='local')
    cert_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "/tmp/rayfed/test-certs/"
    )
    cert_config = {
        "ca_cert": os.path.join(cert_dir, "server.crt"),
        "cert": os.path.join(cert_dir, "server.crt"),
        "key": os.path.join(cert_dir, "server.key"),
    }

    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party, tls_config=cert_config)

    my1 = My.party("alice").remote()
    my2 = My.party("bob").remote()
    x = my1.incr.remote(10)
    y = my2.incr.remote(20)
    o = add.party("alice").remote(x, y)
    assert fed.get(o) == 30
    fed.shutdown()
    ray.shutdown()


def test_enable_tls_across_parties():
    p_alice = multiprocessing.Process(target=_run, args=('alice',))
    p_bob = multiprocessing.Process(target=_run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
