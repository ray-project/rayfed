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
import fed.config as fed_config


def run():
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
    }
    fed.init(addresses=addresses, party="alice")
    config = fed_config.get_cluster_config()
    assert config.cluster_addresses == addresses
    assert config.current_party == "alice"
    fed.shutdown()
    ray.shutdown()


def test_fed_apis():
    p_alice = multiprocessing.Process(target=run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


def _run():
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
    }
    fed.init(addresses=addresses, party="alice")

    @fed.remote
    class MyActor:
        pass

    with pytest.raises(ValueError):
        MyActor.remote()

    fed.shutdown()
    ray.shutdown()


def test_miss_party_name_on_actor():
    p_alice = multiprocessing.Process(target=_run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
