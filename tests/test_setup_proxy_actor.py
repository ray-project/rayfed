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

from fed.config import CrossSiloCommConfig


def run(party):
    compatible_utils.init_ray(address='local', resources={"127.0.0.1": 2})
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    send_proxy_resources = {
        "127.0.0.1": 1
    }
    recv_proxy_resources = {
         "127.0.0.1": 1
    }
    fed.init(
        cluster=cluster,
        party=party,
        cross_silo_send_resource_label=send_proxy_resources,
        cross_silo_recv_resource_label=recv_proxy_resources,
    )

    assert ray.get_actor("SendProxyActor") is not None
    assert ray.get_actor(f"RecverProxyActor-{party}") is not None

    fed.shutdown()
    ray.shutdown()


def run_failure(party):
    compatible_utils.init_ray(address='local', resources={"127.0.0.1": 1})
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    send_proxy_resources = {
        "127.0.0.2": 1  # Insufficient resource
    }
    recv_proxy_resources = {
        "127.0.0.2": 1  # Insufficient resource
    }
    with pytest.raises(ray.exceptions.GetTimeoutError):
        fed.init(
            cluster=cluster,
            party=party,
            cross_silo_comm_config=CrossSiloCommConfig(
                send_resource_label=send_proxy_resources,
                recv_resource_label=recv_proxy_resources,
                timeout_in_seconds=10,
            )
        )

    fed.shutdown()
    ray.shutdown()


def test_setup_proxy_success():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


def test_setup_proxy_failed():
    p_alice = multiprocessing.Process(target=run_failure, args=('alice',))
    p_bob = multiprocessing.Process(target=run_failure, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
