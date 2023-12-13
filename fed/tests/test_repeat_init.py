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
class My:
    def foo(self, i: int):
        return f"foo-{i}"

    def bar(self, li):
        assert li[0] == "hello"
        li1 = li[1]
        li2 = li[2]
        assert fed.get(li1[0]) == "foo-0"
        assert li2[0] == "world"

        assert fed.get(li2[1][0]) == "foo-1"
        return True


addresses = {
    'alice': '127.0.0.1:11012',
    'bob': '127.0.0.1:11011',
}


def run(party):
    def _run():
        compatible_utils.init_ray(address='local')
        fed.init(addresses=addresses, party=party)

        my1 = My.party("alice").remote()
        my2 = My.party("bob").remote()
        o1 = my1.foo.remote(0)
        o2 = my2.foo.remote(1)
        li = ["hello", [o1], ["world", [o2]]]
        o3 = my2.bar.remote(li)

        result = fed.get(o3)
        assert result

        fed.shutdown()
        ray.shutdown()

    _run()
    _run()


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
