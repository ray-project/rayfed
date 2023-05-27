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
import ray


@fed.remote
def dummpy():
    return 2


def run(party):
    cluster = {
        'alice': {
            'address': '127.0.0.1:11010',
            'grpc_options': [
                ('grpc.default_authority', 'alice'),
                ('grpc.max_send_message_length', 200)
            ]
            },
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(
        address='local',
        cluster=cluster,
        party=party,
        cross_silo_messages_max_size_in_bytes=100,
    )

    def _assert_on_send_proxy(proxy_actor):
        alice_config = ray.get(proxy_actor.setup_grpc_config.remote('alice'))
        # print(f"【NKcqx】alice config: {alice_config}")
        assert 'grpc_options' in alice_config
        alice_options = alice_config['grpc_options']
        assert 'grpc.max_send_message_length' in alice_options
        # This should be overwritten by cluster config
        assert alice_options['grpc.max_send_message_length'] == 200
        assert 'grpc.default_authority' in alice_options
        assert alice_options['grpc.default_authority'] == 'alice'

        bob_config = ray.get(proxy_actor.setup_grpc_config.remote('bob'))
        # print(f"【NKcqx】bob config: {bob_config}")
        assert 'grpc_options' in bob_config
        bob_options = bob_config['grpc_options']
        assert "grpc.max_send_message_length" in bob_options
        # Not setting bob's grpc_options, should be the same with global
        assert bob_options["grpc.max_send_message_length"] == 100
        assert 'grpc.default_authority' not in bob_options

    send_proxy = ray.get_actor("SendProxyActor")
    _assert_on_send_proxy(send_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()


def test_grpc_options():
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
