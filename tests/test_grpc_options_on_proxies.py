# Copyright 2022 The RayFed Team
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
import ray


def run(party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(
        address='local',
        cluster=cluster,
        party=party,
        cross_silo_messages_max_size_in_bytes=10,
    )

    def _assert_on_proxy(proxy_actor):
        options = ray.get(proxy_actor._get_grpc_options.remote())
        assert options[0][0] == "grpc.max_send_message_length"
        assert options[0][1] == 10

    send_proxy = ray.get_actor("SendProxyActor")
    recver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    _assert_on_proxy(send_proxy)
    _assert_on_proxy(recver_proxy)

    fed.shutdown()


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
