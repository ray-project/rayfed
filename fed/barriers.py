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
import logging
import threading
import time
from typing import Dict

import cloudpickle
import grpc
import ray

import fed.config as fed_config
import fed.utils as fed_utils
from fed._private.constants import RAYFED_DATE_FMT, RAYFED_LOG_FMT
from fed._private.grpc_options import get_grpc_options, set_max_message_length
from fed.cleanup import push_to_sending
from fed.grpc import fed_pb2, fed_pb2_grpc
from fed.utils import setup_logger

logger = logging.getLogger(__name__)


def key_exists_in_two_dim_dict(the_dict, key_a, key_b) -> bool:
    key_a, key_b = str(key_a), str(key_b)
    if key_a not in the_dict:
        return False
    return key_b in the_dict[key_a]


def add_two_dim_dict(the_dict, key_a, key_b, val):
    key_a, key_b = str(key_a), str(key_b)
    if key_a in the_dict:
        the_dict[key_a].update({key_b: val})
    else:
        the_dict.update({key_a: {key_b: val}})


def get_from_two_dim_dict(the_dict, key_a, key_b):
    key_a, key_b = str(key_a), str(key_b)
    return the_dict[key_a][key_b]


def pop_from_two_dim_dict(the_dict, key_a, key_b):
    key_a, key_b = str(key_a), str(key_b)
    return the_dict[key_a].pop(key_b)


class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, all_events, all_data, party, lock):
        self._events = all_events
        self._all_data = all_data
        self._party = party
        self._lock = lock

    async def SendData(self, request, context):
        upstream_seq_id = request.upstream_seq_id
        downstream_seq_id = request.downstream_seq_id
        logger.debug(
            f'Received a grpc data request from {upstream_seq_id} to '
            f'{downstream_seq_id}.'
        )

        with self._lock:
            add_two_dim_dict(
                self._all_data, upstream_seq_id, downstream_seq_id, request.data
            )
            if not key_exists_in_two_dim_dict(
                self._events, upstream_seq_id, downstream_seq_id
            ):
                event = asyncio.Event()
                add_two_dim_dict(
                    self._events, upstream_seq_id, downstream_seq_id, event
                )
        event = get_from_two_dim_dict(self._events, upstream_seq_id, downstream_seq_id)
        event.set()
        logger.debug(f"Event set for {upstream_seq_id}")
        return fed_pb2.SendDataResponse(result="OK")


async def _run_grpc_server(
    port, event, all_data, party, lock, tls_config=None, grpc_options=None
):
    server = grpc.aio.server(options=grpc_options)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        SendDataService(event, all_data, party, lock), server
    )

    tls_enabled = fed_utils.tls_enabled(tls_config)
    if tls_enabled:
        ca_cert, private_key, cert_chain = fed_utils.load_cert_config(tls_config)
        server_credentials = grpc.ssl_server_credentials(
            [(private_key, cert_chain)],
            root_certificates=ca_cert,
            require_client_auth=ca_cert is not None,
        )
        server.add_secure_port(f'[::]:{port}', server_credentials)
    else:
        server.add_insecure_port(f'[::]:{port}')

    await server.start()
    logger.info(
        f'Successfully start Grpc service with{"out" if not tls_enabled else ""} '
        'credentials.'
    )
    await server.wait_for_termination()


async def send_data_grpc(
    dest,
    data,
    upstream_seq_id,
    downstream_seq_id,
    metadata=None,
    tls_config=None,
    retry_policy=None,
):
    tls_enabled = fed_utils.tls_enabled(tls_config)
    grpc_options = get_grpc_options(retry_policy=retry_policy)
    cluster_config = fed_config.get_cluster_config()
    metadata = fed_utils.dict2tuple(metadata)
    if tls_enabled:
        ca_cert, private_key, cert_chain = fed_utils.load_cert_config(tls_config)
        credentials = grpc.ssl_channel_credentials(
            certificate_chain=cert_chain,
            private_key=private_key,
            root_certificates=ca_cert,
        )

        async with grpc.aio.secure_channel(
            dest,
            credentials,
            options=grpc_options
            + [
                # ('grpc.ssl_target_name_override', "rayfed"),
                # ("grpc.default_authority", "rayfed"),
            ],
        ) as channel:
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            data = cloudpickle.dumps(data)
            request = fed_pb2.SendDataRequest(
                data=data,
                upstream_seq_id=str(upstream_seq_id),
                downstream_seq_id=str(downstream_seq_id),
            )
            # wait for downstream's reply
            response = await stub.SendData(
                request, metadata=metadata, timeout=cluster_config.cross_silo_timeout)
            logger.debug(
                f'Received data response from seq_id {downstream_seq_id}, '
                f'result: {response.result}.'
            )
            return response.result
    else:
        async with grpc.aio.insecure_channel(dest, options=grpc_options) as channel:
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            data = cloudpickle.dumps(data)
            request = fed_pb2.SendDataRequest(
                data=data,
                upstream_seq_id=str(upstream_seq_id),
                downstream_seq_id=str(downstream_seq_id),
            )
            # wait for downstream's reply
            response = await stub.SendData(
                request, metadata=metadata, timeout=cluster_config.cross_silo_timeout)
            logger.debug(
                f'Received data response from seq_id {downstream_seq_id} '
                f'result: {response.result}.'
            )
            return response.result


@ray.remote
class SendProxyActor:
    def __init__(
        self,
        cluster: Dict,
        party: str,
        tls_config: Dict = None,
        logging_level: str = None,
        retry_policy: Dict = None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=RAYFED_LOG_FMT,
            date_format=RAYFED_DATE_FMT,
            party_val=party,
        )
        self._stats = {"send_op_count": 0}
        self._cluster = cluster
        self._party = party
        self._tls_config = tls_config
        self.retry_policy = retry_policy
        self._grpc_metadata = fed_config.get_job_config().grpc_metadata
        cluster_config = fed_config.get_cluster_config()
        set_max_message_length(cluster_config.cross_silo_messages_max_size)

    async def is_ready(self):
        return True

    async def send(
        self,
        dest_party,
        data,
        upstream_seq_id,
        downstream_seq_id,
    ):
        self._stats["send_op_count"] += 1
        assert (
            dest_party in self._cluster
        ), f'Failed to find {dest_party} in cluster {self._cluster}.'
        send_log_msg = (
            f'send data to seq_id {downstream_seq_id} of {dest_party} '
            f'from {upstream_seq_id}'
        )
        logger.debug(
            f'Sending {send_log_msg} with{"out" if not self._tls_config else ""}'
            ' credentials.'
        )
        dest_addr = self._cluster[dest_party]['address']
        try:
            response = await send_data_grpc(
                dest=dest_addr,
                data=data,
                upstream_seq_id=upstream_seq_id,
                downstream_seq_id=downstream_seq_id,
                metadata=self._grpc_metadata,
                tls_config=self._tls_config,
                retry_policy=self.retry_policy,
            )
        except Exception as e:
            logger.error(f'Failed to {send_log_msg}, error: {e}')
            return False
        logger.debug(f"Succeeded to send {send_log_msg}. Response is {response}")
        return True  # True indicates it's sent successfully.

    async def _get_stats(self):
        return self._stats

    async def _get_grpc_options(self):
        return get_grpc_options()


@ray.remote
class RecverProxyActor:
    def __init__(
        self,
        listen_addr: str,
        party: str,
        logging_level: str,
        tls_config=None,
        retry_policy: Dict = None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=RAYFED_LOG_FMT,
            date_format=RAYFED_DATE_FMT,
            party_val=party,
        )
        self._stats = {"receive_op_count": 0}
        self._listen_addr = listen_addr
        self._party = party
        self._tls_config = tls_config
        self.retry_policy = retry_policy
        config = fed_config.get_cluster_config()
        set_max_message_length(config.cross_silo_messages_max_size)
        # Workaround the threading coordinations

        # All events for grpc waitting usage.
        self._events = {}  # map from (upstream_seq_id, downstream_seq_id) to event
        self._all_data = {}  # map from (upstream_seq_id, downstream_seq_id) to data
        self._lock = threading.Lock()

    async def run_grpc_server(self):
        return await _run_grpc_server(
            self._listen_addr[self._listen_addr.index(':') + 1 :],
            self._events,
            self._all_data,
            self._party,
            self._lock,
            self._tls_config,
            get_grpc_options(self.retry_policy),
        )

    async def is_ready(self):
        return True

    async def get_data(self, src_aprty, upstream_seq_id, curr_seq_id):
        self._stats["receive_op_count"] += 1
        data_log_msg = f"data for {curr_seq_id} from {upstream_seq_id} of {src_aprty}"
        logger.debug(f"Getting {data_log_msg}")
        with self._lock:
            if not key_exists_in_two_dim_dict(
                self._events, upstream_seq_id, curr_seq_id
            ):
                add_two_dim_dict(
                    self._events, upstream_seq_id, curr_seq_id, asyncio.Event()
                )
        curr_event = get_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)
        await curr_event.wait()
        logging.debug(f"Waited {data_log_msg}.")
        with self._lock:
            data = pop_from_two_dim_dict(self._all_data, upstream_seq_id, curr_seq_id)
            pop_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)

        # NOTE(qwang): This is used to avoid the conflict with pickle5 in Ray.
        import fed._private.serialization_utils as fed_ser_utils
        fed_ser_utils._apply_loads_function_with_whitelist()
        return cloudpickle.loads(data)

    async def _get_stats(self):
        return self._stats

    async def _get_grpc_options(self):
        return get_grpc_options()


def start_recv_proxy(
    cluster: str,
    party: str,
    logging_level: str,
    tls_config=None,
    retry_policy=None,
):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    party_addr = cluster[party]
    listen_addr = party_addr.get('listen_addr', None)
    if not listen_addr:
        listen_addr = party_addr['address']

    recver_proxy_actor = RecverProxyActor.options(
        name=f"RecverProxyActor-{party}", max_concurrency=1000
    ).remote(
        listen_addr=listen_addr,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=retry_policy,
    )
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())
    logger.info("RecverProxy was successfully created.")


_SEND_PROXY_ACTOR = None


def start_send_proxy(
    cluster: Dict,
    party: str,
    logging_level: str,
    tls_config: Dict = None,
    retry_policy=None,
    max_retries=None,
):
    # Create RecevrProxyActor
    global _SEND_PROXY_ACTOR
    if max_retries is not None:
        _SEND_PROXY_ACTOR = SendProxyActor.options(
            name="SendProxyActor",
            max_concurrency=1000,
            max_task_retries=max_retries,
            max_restarts=1,
        )
    else:
        _SEND_PROXY_ACTOR = SendProxyActor.options(
            name="SendProxyActor", max_concurrency=1000
        )
    _SEND_PROXY_ACTOR = _SEND_PROXY_ACTOR.remote(
        cluster=cluster,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=retry_policy,
    )
    assert ray.get(_SEND_PROXY_ACTOR.is_ready.remote())
    logger.info("SendProxy was successfully created.")


def send(
    dest_party,
    data,
    upstream_seq_id,
    downstream_seq_id,
):
    send_proxy = ray.get_actor("SendProxyActor")
    res = send_proxy.send.remote(
        dest_party=dest_party,
        data=data,
        upstream_seq_id=upstream_seq_id,
        downstream_seq_id=downstream_seq_id,
    )
    push_to_sending(res)
    return res


def recv(party: str, src_party: str, upstream_seq_id, curr_seq_id):
    assert party, 'Party can not be None.'
    receiver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    return receiver_proxy.get_data.remote(src_party, upstream_seq_id, curr_seq_id)


def _grpc_ping(party: str, dest: str, tls_config: Dict) -> bool:
    try:
        if tls_config:
            ca_cert, private_key, cert_chain = fed_utils.load_cert_config(tls_config)
            credentials = grpc.ssl_channel_credentials(
                certificate_chain=cert_chain,
                private_key=private_key,
                root_certificates=ca_cert,
            )

            with grpc.secure_channel(
                dest,
                credentials,
            ) as channel:
                stub = fed_pb2_grpc.GrpcServiceStub(channel)
                request = fed_pb2.SendDataRequest(
                    data=b'ping',
                    upstream_seq_id='ping',
                    downstream_seq_id='ping',
                )
                response = stub.SendData(request)
        else:
            with grpc.insecure_channel(dest) as channel:
                stub = fed_pb2_grpc.GrpcServiceStub(channel)
                request = fed_pb2.SendDataRequest(
                    data=b'ping',
                    upstream_seq_id='ping',
                    downstream_seq_id='ping',
                )
                response = stub.SendData(request)
        logger.info(
            f'Succeeded to ping {party} on {dest}, the result: {response.result}.'
        )
        return True
    except Exception as e:
        logger.info(
            f'Failed to ping {party} on {dest}, this could be normal, '
            f'the possible reason is {party} has not yet started.'
        )
        logger.debug(f'Ping error: {e}')
        return False


def ping_others(cluster: Dict[str, Dict], self_party: str, tls_config: Dict):
    """Ping other parties until all are ready or timeout."""
    others = [party for party in cluster if not party == self_party]
    max_retries = 3600
    tried = 0
    while tried < max_retries and others:
        logger.info(
            f'Try ping {others} at {tried} attemp, up to {max_retries} attemps.'
        )
        tried += 1
        others[:] = [
            other
            for other in others
            if not _grpc_ping(other, cluster[other]['address'], tls_config)
        ]
        if others:
            time.sleep(2)
