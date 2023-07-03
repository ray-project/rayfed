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


@fed.remote
def dummpy():
    return 2


def run(party):
    compatible_utils.init_ray(address='local')
    cluster = {
        'alice': {'address': '127.0.0.1:11019'},
        'bob': {'address': '127.0.0.1:11018'},
    }
    fed.init(
        cluster=cluster,
        party=party,
        cross_silo_comm_config=CrossSiloCommConfig(
            messages_max_size_in_bytes=100)
    )

    def _assert_on_proxy(proxy_actor):
        options = ray.get(proxy_actor._get_grpc_options.remote())
        assert options[0][0] == "grpc.max_send_message_length"
        assert options[0][1] == 100
        assert ('grpc.so_reuseport', 0) in options

    send_proxy = ray.get_actor("SendProxyActor")
    recver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    _assert_on_proxy(send_proxy)
    _assert_on_proxy(recver_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_grpc_max_size():
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
