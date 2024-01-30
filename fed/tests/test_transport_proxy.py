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

import asyncio

import cloudpickle
import grpc
import pytest
import ray

import fed._private.compatible_utils as compatible_utils
import fed.utils as fed_utils
from fed._private import constants, global_context
from fed.proxy.barriers import (
    _start_receiver_proxy,
    _start_sender_proxy,
    receiver_proxy_actor_name,
    send,
)
from fed.proxy.grpc.grpc_proxy import GrpcReceiverProxy, GrpcSenderProxy

if compatible_utils._compare_version_strings(
    fed_utils.get_package_version("protobuf"), "4.0.0"
):
    from fed.grpc.pb4 import fed_pb2 as fed_pb2
    from fed.grpc.pb4 import fed_pb2_grpc as fed_pb2_grpc
else:
    from fed.grpc.pb3 import fed_pb2 as fed_pb2
    from fed.grpc.pb3 import fed_pb2_grpc as fed_pb2_grpc


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target receiver proxy, and there also have
    N receivers to `get_data` from receiver proxy at that time.
    """
    compatible_utils.init_ray(address="local")
    test_job_name = "test_n_to_1_transport"
    party = "test_party"
    global_context.init_global_context(party, test_job_name, False, False)
    global_context.get_global_context().get_cleanup_manager().start()
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
    }
    compatible_utils._init_internal_kv(test_job_name)
    compatible_utils.kv.put(
        constants.KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config)
    )

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:12344"

    addresses = {"test_party": SERVER_ADDRESS}
    _start_receiver_proxy(
        addresses,
        party,
        logging_level="info",
        proxy_cls=GrpcReceiverProxy,
        proxy_config={},
    )
    _start_sender_proxy(
        addresses,
        party,
        logging_level="info",
        proxy_cls=GrpcSenderProxy,
        proxy_config={},
    )

    sent_objs = []
    get_objs = []
    receiver_proxy_actor = ray.get_actor(receiver_proxy_actor_name())
    for i in range(NUM_DATA):
        sent_obj = send(party, f"data-{i}", i, i + 1)
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


class TestSendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(
        self, all_events, all_data, party, lock, expected_metadata, expected_jobname
    ):
        self.expected_metadata = expected_metadata or {}
        self._expected_jobname = expected_jobname or ""

    async def SendData(self, request, context):
        job_name = request.job_name
        assert self._expected_jobname == job_name
        metadata = dict(context.invocation_metadata())
        for k, v in self.expected_metadata.items():
            assert (
                k in metadata
            ), f"The expected key {k} is not in the metadata keys: {metadata.keys()}."
            assert v == metadata[k]
        event = asyncio.Event()
        event.set()
        return fed_pb2.SendDataResponse(code=200, result="OK")


async def _test_run_grpc_server(
    port,
    event,
    all_data,
    party,
    lock,
    grpc_options=None,
    expected_metadata=None,
    expected_jobname=None,
):
    server = grpc.aio.server(options=grpc_options)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        TestSendDataService(
            event, all_data, party, lock, expected_metadata, expected_jobname
        ),
        server,
    )
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    await server.wait_for_termination()


@ray.remote
class TestReceiverProxyActor:
    def __init__(
        self,
        listen_addr: str,
        party: str,
        expected_metadata: dict,
        expected_jobname: str,
    ):
        self._listen_addr = listen_addr
        self._party = party
        self._expected_metadata = expected_metadata
        self._expected_jobname = expected_jobname

    async def run_grpc_server(self):
        return await _test_run_grpc_server(
            self._listen_addr[self._listen_addr.index(":") + 1 :],
            None,
            None,
            self._party,
            None,
            expected_metadata=self._expected_metadata,
            expected_jobname=self._expected_jobname,
        )

    async def is_ready(self):
        return True


def _test_start_receiver_proxy(
    addresses: str,
    party: str,
    expected_metadata: dict,
    expected_jobname: str,
):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    address = addresses[party]
    receiver_proxy_actor = TestReceiverProxyActor.options(
        name=receiver_proxy_actor_name(), max_concurrency=1000
    ).remote(
        listen_addr=address,
        party=party,
        expected_metadata=expected_metadata,
        expected_jobname=expected_jobname,
    )
    receiver_proxy_actor.run_grpc_server.remote()
    assert ray.get(receiver_proxy_actor.is_ready.remote())


def test_send_grpc_with_meta():
    compatible_utils.init_ray(address="local")
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
    }
    metadata = {"key": "value"}
    config = {"http_header": metadata}
    job_config = {
        constants.KEY_OF_CROSS_SILO_COMM_CONFIG_DICT: config,
    }
    test_job_name = "test_send_grpc_with_meta"
    party_name = "test_party"
    global_context.init_global_context(party_name, test_job_name, False, False)
    compatible_utils._init_internal_kv(test_job_name)
    compatible_utils.kv.put(
        constants.KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config)
    )
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG, cloudpickle.dumps(job_config))
    global_context.get_global_context().get_cleanup_manager().start()

    SERVER_ADDRESS = "127.0.0.1:12344"

    addresses = {party_name: SERVER_ADDRESS}
    _test_start_receiver_proxy(
        addresses,
        party_name,
        expected_metadata=metadata,
        expected_jobname=test_job_name,
    )
    _start_sender_proxy(
        addresses,
        party_name,
        logging_level="info",
        proxy_cls=GrpcSenderProxy,
        proxy_config=config,
    )
    sent_objs = []
    sent_obj = send(party_name, "data", 0, 1)
    sent_objs.append(sent_obj)
    for result in ray.get(sent_objs):
        assert result

    global_context.get_global_context().get_cleanup_manager().stop()
    global_context.clear_global_context()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
