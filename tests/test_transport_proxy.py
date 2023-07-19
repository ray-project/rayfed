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
import pytest
import ray
import grpc

import fed.utils as fed_utils
import fed._private.compatible_utils as compatible_utils
from fed.config import CrossSiloMessageConfig, GrpcCrossSiloMessageConfig
from fed._private import constants
from fed._private import global_context
from fed.proxy.barriers import (
    send,
    _start_receiver_proxy,
    _start_sender_proxy
)
from fed.proxy.grpc.grpc_proxy import GrpcSenderProxy, GrpcReceiverProxy
if compatible_utils._compare_version_strings(
        fed_utils.get_package_version('protobuf'), '4.0.0'):
    from fed.grpc import fed_pb2_in_protobuf4 as fed_pb2
    from fed.grpc import fed_pb2_grpc_in_protobuf4 as fed_pb2_grpc
else:
    from fed.grpc import fed_pb2_in_protobuf3 as fed_pb2
    from fed.grpc import fed_pb2_grpc_in_protobuf3 as fed_pb2_grpc


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target receiver proxy, and there also have
    N receivers to `get_data` from receiver proxy at that time.
    """
    compatible_utils.init_ray(address='local')

    global_context.get_global_context().get_cleanup_manager().start()
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
    }
    compatible_utils._init_internal_kv()
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    config = GrpcCrossSiloMessageConfig()
    _start_receiver_proxy(
        cluster_config,
        party,
        logging_level='info',
        proxy_cls=GrpcReceiverProxy,
        proxy_config=config
    )
    _start_sender_proxy(
        cluster_config,
        party,
        logging_level='info',
        proxy_cls=GrpcSenderProxy,
        proxy_config=config
    )

    sent_objs = []
    get_objs = []
    receiver_proxy_actor = ray.get_actor(f"ReceiverProxyActor-{party}")
    for i in range(NUM_DATA):
        sent_obj = send(party, f"data-{i}", i, i + 1)
        sent_objs.append(sent_obj)
        get_obj = receiver_proxy_actor.get_data.remote(party, i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in ray.get(get_objs)

    global_context.get_global_context().get_cleanup_manager().graceful_stop()
    global_context.clear_global_context()
    ray.shutdown()


class TestSendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, all_events, all_data, party, lock, expected_metadata):
        self.expected_metadata = expected_metadata or {}

    async def SendData(self, request, context):
        metadata = dict(context.invocation_metadata())
        for k, v in self.expected_metadata.items():
            assert k in metadata
            assert v == metadata[k]
        event = asyncio.Event()
        event.set()
        return fed_pb2.SendDataResponse(result="OK")


async def _test_run_grpc_server(
    port, event, all_data, party, lock, tls_config=None, grpc_options=None,
    expected_metadata=None
):
    server = grpc.aio.server(options=grpc_options)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        TestSendDataService(event, all_data, party, lock, expected_metadata), server
    )
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    await server.wait_for_termination()


@ray.remote
class TestReceiverProxyActor:
    def __init__(
        self,
        listen_addr: str,
        party: str,
        expected_metadata: dict,
    ):
        self._listen_addr = listen_addr
        self._party = party
        self._expected_metadata = expected_metadata

    async def run_grpc_server(self):
        return await _test_run_grpc_server(
            self._listen_addr[self._listen_addr.index(':') + 1 :],
            None,
            None,
            self._party,
            None,
            expected_metadata=self._expected_metadata,
        )

    async def is_ready(self):
        return True


def _test_start_receiver_proxy(
    cluster: str,
    party: str,
    logging_level: str,
    expected_metadata: dict,
):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    party_addr = cluster[party]
    listen_addr = party_addr.get('listen_addr', None)
    if not listen_addr:
        listen_addr = party_addr['address']

    receiver_proxy_actor = TestReceiverProxyActor.options(
        name=f"ReceiverProxyActor-{party}", max_concurrency=1000
    ).remote(
        listen_addr=listen_addr,
        party=party,
        expected_metadata=expected_metadata
    )
    receiver_proxy_actor.run_grpc_server.remote()
    assert ray.get(receiver_proxy_actor.is_ready.remote())


def test_send_grpc_with_meta():
    compatible_utils.init_ray(address='local')
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
    }
    metadata = {"key": "value"}
    sender_proxy_config = CrossSiloMessageConfig(
        http_header=metadata
    )
    job_config = {
        constants.KEY_OF_CROSS_SILO_MESSAGE_CONFIG:
            sender_proxy_config,
    }
    compatible_utils._init_internal_kv()
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG,
                            cloudpickle.dumps(job_config))
    global_context.get_global_context().get_cleanup_manager().start()

    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    _test_start_receiver_proxy(
        cluster_config, party, logging_level='info',
        expected_metadata=metadata,
    )
    _start_sender_proxy(
        cluster_config,
        party,
        logging_level='info',
        proxy_cls=GrpcSenderProxy,
        proxy_config=GrpcCrossSiloMessageConfig())
    sent_objs = []
    sent_obj = send(party, "data", 0, 1)
    sent_objs.append(sent_obj)
    for result in ray.get(sent_objs):
        assert result

    global_context.get_global_context().get_cleanup_manager().graceful_stop()
    global_context.clear_global_context()
    ray.shutdown()


def test_send_grpc_with_party_specific_meta():
    compatible_utils.init_ray(address='local')
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
    }
    sender_proxy_config = CrossSiloMessageConfig(
        http_header={"key": "value"})
    job_config = {
        constants.KEY_OF_CROSS_SILO_MESSAGE_CONFIG:
            sender_proxy_config,
    }
    compatible_utils._init_internal_kv()
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG,
                            cloudpickle.dumps(job_config))
    global_context.get_global_context().get_cleanup_manager().start()

    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_parties_config = {
        'test_party': {
            'address': SERVER_ADDRESS,
            'cross_silo_message_config': CrossSiloMessageConfig(
                http_header={"token": "test-party-token"})
        }
    }
    _test_start_receiver_proxy(
        cluster_parties_config, party, logging_level='info',
        expected_metadata={"key": "value", "token": "test-party-token"},
    )
    _start_sender_proxy(
        cluster_parties_config,
        party,
        logging_level='info',
        proxy_cls=GrpcSenderProxy,
        proxy_config=sender_proxy_config)
    sent_objs = []
    sent_obj = send(party, "data", 0, 1)
    sent_objs.append(sent_obj)
    for result in ray.get(sent_objs):
        assert result

    global_context.get_global_context().get_cleanup_manager().graceful_stop()
    global_context.clear_global_context()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
