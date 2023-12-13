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
def dummpy():
    return 2


def run(party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11019',
        'bob': '127.0.0.1:11018',
    }
    fed.init(
        addresses=addresses,
        party=party,
        config={
            "cross_silo_comm": {
                "grpc_channel_options": [('grpc.max_send_message_length', 100)],
            },
        },
    )

    def _assert_on_proxy(proxy_actor):
        config = ray.get(proxy_actor._get_proxy_config.remote())
        options = config['grpc_options']
        assert ("grpc.max_send_message_length", 100) in options
        assert ('grpc.so_reuseport', 0) in options

    sender_proxy = ray.get_actor(sender_proxy_actor_name())
    receiver_proxy = ray.get_actor(receiver_proxy_actor_name())
    _assert_on_proxy(sender_proxy)
    _assert_on_proxy(receiver_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_grpc_max_size_by_channel_options():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


def run2(party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11019',
        'bob': '127.0.0.1:11018',
    }
    fed.init(
        addresses=addresses,
        party=party,
        config={
            "cross_silo_comm": {
                "messages_max_size_in_bytes": 100,
            },
        },
    )

    def _assert_on_proxy(proxy_actor):
        config = ray.get(proxy_actor._get_proxy_config.remote())
        options = config['grpc_options']
        assert ("grpc.max_send_message_length", 100) in options
        assert ("grpc.max_receive_message_length", 100) in options
        assert ('grpc.so_reuseport', 0) in options

    sender_proxy = ray.get_actor(sender_proxy_actor_name())
    receiver_proxy = ray.get_actor(receiver_proxy_actor_name())
    _assert_on_proxy(sender_proxy)
    _assert_on_proxy(receiver_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_grpc_max_size_by_common_config():
    p_alice = multiprocessing.Process(target=run2, args=('alice',))
    p_bob = multiprocessing.Process(target=run2, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


def run3(party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11019',
        'bob': '127.0.0.1:11018',
    }
    fed.init(
        addresses=addresses,
        party=party,
        config={
            "cross_silo_comm": {
                "messages_max_size_in_bytes": 100,
                "grpc_channel_options": [
                    ('grpc.max_send_message_length', 200),
                ],
            },
        },
    )

    def _assert_on_proxy(proxy_actor):
        config = ray.get(proxy_actor._get_proxy_config.remote())
        options = config['grpc_options']
        assert ("grpc.max_send_message_length", 200) in options
        assert ("grpc.max_receive_message_length", 100) in options
        assert ('grpc.so_reuseport', 0) in options

    sender_proxy = ray.get_actor(sender_proxy_actor_name())
    receiver_proxy = ray.get_actor(receiver_proxy_actor_name())
    _assert_on_proxy(sender_proxy)
    _assert_on_proxy(receiver_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()
    ray.shutdown()


def test_grpc_max_size_by_both_config():
    p_alice = multiprocessing.Process(target=run3, args=('alice',))
    p_bob = multiprocessing.Process(target=run3, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
