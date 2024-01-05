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
from fed.proxy.barriers import receiver_proxy_actor_name, sender_proxy_actor_name


@fed.remote
def f():
    return "hello"


@fed.remote
def g(x, index):
    return x + str(index)


def run(party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party)

    o = f.party("alice").remote()
    o1 = g.party("bob").remote(o, 1)
    o2 = g.party("bob").remote(o, 2)

    a, b, c = fed.get([o, o1, o2])
    assert a == "hello"
    assert b == "hello1"
    assert c == "hello2"

    if party == "bob":
        proxy_actor = ray.get_actor(receiver_proxy_actor_name())
        stats = ray.get(proxy_actor._get_stats.remote())
        assert stats["receive_op_count"] == 1
    if party == "alice":
        proxy_actor = ray.get_actor(sender_proxy_actor_name())
        stats = ray.get(proxy_actor._get_stats.remote())
        assert stats["send_op_count"] == 1
    fed.shutdown()
    ray.shutdown()


def test_cache_fed_object_if_sent():
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
