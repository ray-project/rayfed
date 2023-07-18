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

from fed.config import GrpcCrossSiloMsgConfig


@fed.remote
def dummpy():
    return 2


def run(party):
    compatible_utils.init_ray(address='local')
    cluster = {
        'alice': {
            'address': '127.0.0.1:11010',
            'cross_silo_comm_config': GrpcCrossSiloMsgConfig(
                grpc_channel_options=[
                    ('grpc.default_authority', 'alice'),
                    ('grpc.max_send_message_length', 200)
                ])
            },
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(
        cluster=cluster,
        party=party,
        global_cross_silo_msg_config=GrpcCrossSiloMsgConfig(
            grpc_channel_options=[(
                'grpc.max_send_message_length', 100
            )]
        )
    )

    def _assert_on_send_proxy(proxy_actor):
        alice_config = ray.get(proxy_actor._get_proxy_config.remote('alice'))
        # print(f"【NKcqx】alice config: {alice_config}")
        assert 'grpc_options' in alice_config
        alice_options = alice_config['grpc_options']
        assert ('grpc.max_send_message_length', 200) in alice_options
        assert ('grpc.default_authority', 'alice') in alice_options

        bob_config = ray.get(proxy_actor._get_proxy_config.remote('bob'))
        assert 'grpc_options' in bob_config
        bob_options = bob_config['grpc_options']
        assert ('grpc.max_send_message_length', 100) in bob_options
        assert not any(o[0] == 'grpc.default_authority' for o in bob_options)

    send_proxy = ray.get_actor("SendProxyActor")
    _assert_on_send_proxy(send_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_grpc_options():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


def party_grpc_options(party):
    compatible_utils.init_ray(address='local')
    cluster = {
        'alice': {
            'address': '127.0.0.1:11010',
            'cross_silo_comm_config': GrpcCrossSiloMsgConfig(
                grpc_channel_options=[
                    ('grpc.default_authority', 'alice'),
                    ('grpc.max_send_message_length', 51 * 1024 * 1024)
                ])
        },
        'bob': {
            'address': '127.0.0.1:11011',
            'cross_silo_comm_config': GrpcCrossSiloMsgConfig(
                grpc_channel_options=[
                    ('grpc.default_authority', 'bob'),
                    ('grpc.max_send_message_length', 50 * 1024 * 1024)
                ])
        },
    }
    fed.init(
        cluster=cluster,
        party=party,
        global_cross_silo_msg_config=GrpcCrossSiloMsgConfig(
            grpc_channel_options=[(
                'grpc.max_send_message_length', 100
            )]
        )
    )

    def _assert_on_send_proxy(proxy_actor):
        alice_config = ray.get(proxy_actor._get_proxy_config.remote('alice'))
        assert 'grpc_options' in alice_config
        alice_options = alice_config['grpc_options']
        assert ('grpc.max_send_message_length', 51 * 1024 * 1024) in alice_options
        assert ('grpc.default_authority', 'alice') in alice_options

        bob_config = ray.get(proxy_actor._get_proxy_config.remote('bob'))
        assert 'grpc_options' in bob_config
        bob_options = bob_config['grpc_options']
        assert ('grpc.max_send_message_length', 50 * 1024 * 1024) in bob_options
        assert ('grpc.default_authority', 'bob') in bob_options

    send_proxy = ray.get_actor("SendProxyActor")
    _assert_on_send_proxy(send_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_party_specific_grpc_options():
    p_alice = multiprocessing.Process(
        target=party_grpc_options, args=('alice',))
    p_bob = multiprocessing.Process(
        target=party_grpc_options, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
