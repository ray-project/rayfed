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

import numpy
import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils


@fed.remote
def generate_wrong_type():
    class WrongType:
        pass

    return WrongType()


@fed.remote
def generate_allowed_type():
    return numpy.array([1, 2, 3, 4, 5])


@fed.remote
def pass_arg(d):
    return True


def run(party):
    compatible_utils.init_ray(address='local', include_dashboard=False)
    addresses = {
        'alice': '127.0.0.1:11355',
        'bob': '127.0.0.1:11356',
    }
    allowed_list = {
        "numpy.core.numeric": ["*"],
        "numpy": ["dtype"],
    }
    fed.init(
        addresses=addresses,
        party=party,
        config={"cross_silo_comm": {'serializing_allowed_list': allowed_list}},
    )

    # Test passing an allowed type.
    o1 = generate_allowed_type.party("alice").remote()
    o2 = pass_arg.party("bob").remote(o1)
    res = fed.get(o2)
    assert res

    # Test passing an unallowed type.
    o3 = generate_wrong_type.party("alice").remote()
    o4 = pass_arg.party("bob").remote(o3)
    if party == "bob":
        try:
            fed.get(o4)
            assert False, "This code path shouldn't be reached."
        except Exception as e:
            assert "_pickle.UnpicklingError" in str(e)
    else:
        import time

        time.sleep(10)
    fed.shutdown()
    ray.shutdown()


def test_restricted_loads():
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
