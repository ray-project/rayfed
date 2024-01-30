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

import os

import cloudpickle
import pytest
import ray

import fed._private.compatible_utils as compatible_utils
from fed._private import constants, global_context
from fed.proxy.barriers import (
    _start_receiver_proxy,
    _start_sender_proxy,
    receiver_proxy_actor_name,
    send,
)
from fed.proxy.grpc.grpc_proxy import GrpcReceiverProxy, GrpcSenderProxy


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target receiver proxy, and there also have
    N receivers to `get_data` from receiver proxy at that time.
    """
    compatible_utils.init_ray(address="local")
    test_job_name = "test_n_to_1_transport"
    cert_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "/tmp/rayfed/test-certs/"
    )
    tls_config = {
        "ca_cert": os.path.join(cert_dir, "server.crt"),
        "cert": os.path.join(cert_dir, "server.crt"),
        "key": os.path.join(cert_dir, "server.key"),
    }
    party = "test_party"
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: tls_config,
    }
    global_context.init_global_context(party, test_job_name, False, False)
    global_context.get_global_context().get_cleanup_manager().start()
    compatible_utils._init_internal_kv(test_job_name)
    compatible_utils.kv.put(
        constants.KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config)
    )

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:65422"
    addresses = {"test_party": SERVER_ADDRESS}
    _start_receiver_proxy(
        addresses,
        party,
        logging_level="info",
        tls_config=tls_config,
        proxy_cls=GrpcReceiverProxy,
        proxy_config={},
    )
    _start_sender_proxy(
        addresses,
        party,
        logging_level="info",
        tls_config=tls_config,
        proxy_cls=GrpcSenderProxy,
        proxy_config={},
    )

    sent_objs = []
    get_objs = []
    receiver_proxy_actor = ray.get_actor(receiver_proxy_actor_name())
    for i in range(NUM_DATA):
        sent_obj = send(
            party,
            f"data-{i}",
            i,
            i + 1,
        )
        sent_objs.append(sent_obj)
        get_obj = receiver_proxy_actor.get_data.remote(party, i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in ray.get(get_objs)

    global_context.get_global_context().get_cleanup_manager().stop()
    global_context.clear_global_context()
    compatible_utils._clear_internal_kv()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
