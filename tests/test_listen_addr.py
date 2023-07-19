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
def f():
    return 100


@fed.remote
class My:
    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value


def run(party, is_inner_party):
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': {'address': '127.0.0.1:11012', 'listening_on': '0.0.0.0:11012'},
        'bob': {'address': '127.0.0.1:11011', 'listening_on': '0.0.0.0:11011'},
    }
    fed.init(addresses=addresses, party=party)

    o = f.party("alice").remote()
    actor_location = "alice" if is_inner_party else "bob"
    my = My.party(actor_location).remote(o)
    val = my.get_value.remote()
    result = fed.get(val)
    assert result == 100
    assert fed.get(o) == 100
    import time

    time.sleep(5)
    fed.shutdown()
    ray.shutdown()


def test_listen_addr():
    p_alice = multiprocessing.Process(target=run, args=('alice', True))
    p_bob = multiprocessing.Process(target=run, args=('bob', True))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


def test_listen_used_addr():
    def run(party):
        import socket

        compatible_utils.init_ray(address='local')
        occupied_port = 11020
        # NOTE(NKcqx): Firstly try to bind IPv6 because the grpc server will do so.
        # Otherwise this UT will false because socket bind $occupied_port
        # on IPv4 address while grpc server listendn Ipv6 address.
        try:
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            # Pre-occuping the port
            s.bind(("::", occupied_port))
        except OSError:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", occupied_port))

        cluster = {
            'alice': {
                'address': '127.0.0.1:11012',
                'listen_addr': f'0.0.0.0:{occupied_port}'},
            'bob': {
                'address': '127.0.0.1:11011',
                'listen_addr': '0.0.0.0:11011'},
        }

        # Starting grpc server on an used port will cause AssertionError
        with pytest.raises(AssertionError):
            fed.init(addresses=addresses, party=party)

        import time

        time.sleep(5)
        s.close()
        fed.shutdown()
        ray.shutdown()

    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    # import sys

    # sys.exit(pytest.main(["-sv", __file__]))
    test_listen_used_addr()
