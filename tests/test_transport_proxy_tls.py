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

import os
import pytest

import ray
import cloudpickle
import ray.experimental.internal_kv as internal_kv

from fed.barriers import RecverProxyActor, send, start_send_proxy
from fed.cleanup import wait_sending
from fed._private.constants import RAYFED_TLS_CONFIG


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target recver proxy, and there also have
    N receivers to `get_data` from Recver proxy at that time.
    """
    ray.init(address='local')

    cert_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "/tmp/rayfed/test-certs/"
    )
    tls_config = {
        "ca_cert": os.path.join(cert_dir, "server.crt"),
        "cert": os.path.join(cert_dir, "server.crt"),
        "key": os.path.join(cert_dir, "server.key"),
    }
    internal_kv._internal_kv_put(RAYFED_TLS_CONFIG, cloudpickle.dumps(tls_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:65422"
    recver_proxy_actor = RecverProxyActor.options(
        name="RecverProxyActor-TEST", max_concurrency=2000
    ).remote(SERVER_ADDRESS, "test_party", tls_config)
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())
    start_send_proxy(
        {'test_party': {'address': SERVER_ADDRESS}}, 'test_party', tls_config
    )

    sent_objs = []
    get_objs = []
    for i in range(NUM_DATA):
        sent_obj = send(
            'test_party',
            f"data-{i}",
            i,
            i + 1,
        )
        sent_objs.append(sent_obj)
        get_obj = recver_proxy_actor.get_data.remote(i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in ray.get(get_objs)

    wait_sending()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
