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


@fed.remote
def foo(i: int):
    return f"foo-{i}"


@fed.remote
def bar(li):
    assert li[0] == "hello"
    li1 = li[1]
    li2 = li[2]
    assert fed.get(li1[0]) == "foo-0"
    assert li2[0] == "world"
    assert fed.get(li2[1][0]) == "foo-1"
    return True


def run(party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party)
    o1 = foo.party("alice").remote(0)
    o2 = foo.party("bob").remote(1)
    li = ["hello", [o1], ["world", [o2]]]
    o3 = bar.party("bob").remote(li)

    result = fed.get(o3)
    assert result
    fed.shutdown()
    ray.shutdown()


def test_pass_fed_objects_in_list():
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
