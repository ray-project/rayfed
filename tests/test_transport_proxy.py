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


import fed._private.compatible_utils as compatible_utils
from fed._private import constants
from fed.grpc import fed_pb2, fed_pb2_grpc
from fed.barriers import send, start_recv_proxy, start_send_proxy
from fed.cleanup import wait_sending


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target recver proxy, and there also have
    N receivers to `get_data` from Recver proxy at that time.
    """
    compatible_utils.init_ray(address='local')

    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
        constants.KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES: None,
        constants.KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST: {},
        constants.KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS: 60,
    }
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    start_recv_proxy(
        cluster_config,
        party,
        logging_level='info',
    )
    start_send_proxy(cluster_config, party, logging_level='info')

    sent_objs = []
    get_objs = []
    recver_proxy_actor = ray.get_actor(f"RecverProxyActor-{party}")
    for i in range(NUM_DATA):
        sent_obj = send(party, f"data-{i}", i, i + 1)
        sent_objs.append(sent_obj)
        get_obj = recver_proxy_actor.get_data.remote(party, i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in ray.get(get_objs)

    wait_sending()
    ray.shutdown()


class TestSendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, all_events, all_data, party, lock):
        pass

    async def SendData(self, request, context):
        metadata = dict(context.invocation_metadata())
        assert 'key' in metadata
        assert 'value' == metadata['key']
        event = asyncio.Event()
        event.set()
        return fed_pb2.SendDataResponse(result="OK")


async def _test_run_grpc_server(
    port, event, all_data, party, lock, tls_config=None, grpc_options=None
):
    server = grpc.aio.server(options=grpc_options)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        TestSendDataService(event, all_data, party, lock), server
    )
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    await server.wait_for_termination()


@ray.remote
class TestRecverProxyActor:
    def __init__(
        self,
        listen_addr: str,
        party: str,
    ):
        self._listen_addr = listen_addr
        self._party = party

    async def run_grpc_server(self):
        return await _test_run_grpc_server(
            self._listen_addr[self._listen_addr.index(':') + 1 :],
            None,
            None,
            self._party,
            None,
        )

    async def is_ready(self):
        return True


def _test_start_recv_proxy(
    cluster: str,
    party: str,
    logging_level: str,
):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    party_addr = cluster[party]
    listen_addr = party_addr.get('listen_addr', None)
    if not listen_addr:
        listen_addr = party_addr['address']

    recver_proxy_actor = TestRecverProxyActor.options(
        name=f"RecverProxyActor-{party}", max_concurrency=1000
    ).remote(
        listen_addr=listen_addr,
        party=party,
    )
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())


def test_send_grpc_with_meta():
    compatible_utils.init_ray(address='local')
    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: "",
        constants.KEY_OF_CURRENT_PARTY_NAME: "",
        constants.KEY_OF_TLS_CONFIG: "",
        constants.KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES: None,
        constants.KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST: {},
        constants.KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS: 60,
    }
    job_config = {
        constants.KEY_OF_GRPC_METADATA: {
            "key": "value"
        }
    }
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG,
                            cloudpickle.dumps(job_config))

    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    _test_start_recv_proxy(cluster_config, party, logging_level='info')
    start_send_proxy(cluster_config, party, logging_level='info')
    sent_objs = []
    sent_obj = send(party, "data", 0, 1)
    sent_objs.append(sent_obj)
    for result in ray.get(sent_objs):
        assert result

    wait_sending()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
