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


def _run(party):
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

    addresses = {
        'alice': f'127.0.0.1:{occupied_port}'
    }

    # Starting grpc server on an used port will cause AssertionError
    with pytest.raises(AssertionError):
        fed.init(
            addresses=addresses,
            party=party,
        )

    import time

    time.sleep(5)
    s.close()
    fed.shutdown()
    ray.shutdown()


def test_listen_used_address():
    p_alice = multiprocessing.Process(target=_run, args=('alice',))
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    # import sys

    # sys.exit(pytest.main(["-sv", __file__]))
    test_listen_used_address()
