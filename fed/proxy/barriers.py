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
import copy
from typing import Dict, Optional

import cloudpickle
import grpc
import ray

import fed.config as fed_config
import fed.utils as fed_utils
from fed._private import constants
from fed._private.grpc_options import get_grpc_options, set_max_message_length
# from fed.cleanup import push_to_sending
from fed.config import get_cluster_config
from fed.grpc import fed_pb2, fed_pb2_grpc
from fed.utils import setup_logger
from fed._private.global_context import get_global_context

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
    port, event, all_data, party, lock,
    server_ready_future, tls_config=None, grpc_options=None
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

    msg = f"Succeeded to add port {port}."
    await server.start()
    logger.info(
        f'Successfully start Grpc service with{"out" if not tls_enabled else ""} '
        'credentials.'
    )
    server_ready_future.set_result((True, msg))
    await server.wait_for_termination()


async def send_data_grpc(
    dest,
    data,
    upstream_seq_id,
    downstream_seq_id,
    metadata=None,
    tls_config=None,
    retry_policy=None,
    grpc_options=None
):
    grpc_options = get_grpc_options(retry_policy=retry_policy) if \
                    grpc_options is None else fed_utils.dict2tuple(grpc_options)
    tls_enabled = fed_utils.tls_enabled(tls_config)
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
            options=grpc_options,
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
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
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
        dest_party_grpc_config = self.setup_grpc_config(dest_party)
        try:
            response = await send_data_grpc(
                dest=dest_addr,
                data=data,
                upstream_seq_id=upstream_seq_id,
                downstream_seq_id=downstream_seq_id,
                metadata=dest_party_grpc_config['grpc_metadata'],
                tls_config=self._tls_config,
                retry_policy=self.retry_policy,
                grpc_options=dest_party_grpc_config['grpc_options']
            )
        except Exception as e:
            logger.error(f'Failed to {send_log_msg}, error: {e}')
            return False
        logger.debug(f"Succeeded to send {send_log_msg}. Response is {response}")
        return True  # True indicates it's sent successfully.

    def setup_grpc_config(self, dest_party):
        dest_party_grpc_config = {}
        global_grpc_metadata = (
            dict(self._grpc_metadata) if self._grpc_metadata is not None else {}
        )
        dest_party_grpc_metadata = dict(
            self._cluster[dest_party].get('grpc_metadata', {})
        )
        # merge grpc metadata
        dest_party_grpc_config['grpc_metadata'] = {
            **global_grpc_metadata, **dest_party_grpc_metadata}

        global_grpc_options = dict(get_grpc_options(self.retry_policy))
        dest_party_grpc_options = dict(
            self._cluster[dest_party].get('grpc_options', {})
        )
        dest_party_grpc_config['grpc_options'] = {
            **global_grpc_options, **dest_party_grpc_options}
        return dest_party_grpc_config

    async def _get_stats(self):
        return self._stats

    async def _get_grpc_options(self):
        return get_grpc_options()

    async def _get_cluster_info(self):
        return self._cluster


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
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
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

        # Flag to see whether grpc server starts
        self._server_ready_future = asyncio.Future()

        # All events for grpc waitting usage.
        self._events = {}  # map from (upstream_seq_id, downstream_seq_id) to event
        self._all_data = {}  # map from (upstream_seq_id, downstream_seq_id) to data
        self._lock = threading.Lock()

    async def run_grpc_server(self):
        try:
            port = self._listen_addr[self._listen_addr.index(':') + 1 :]
            await _run_grpc_server(
                port,
                self._events,
                self._all_data,
                self._party,
                self._lock,
                self._server_ready_future,
                self._tls_config,
                get_grpc_options(self.retry_policy),
            )
        except RuntimeError as err:
            msg = f'Grpc server failed to listen to port: {port}' \
                  f' Try another port by setting `listen_addr` into `cluster` config' \
                  f' when calling `fed.init`. Grpc error msg: {err}'
            self._server_ready_future.set_result((False, msg))

    async def is_ready(self):
        await self._server_ready_future
        return self._server_ready_future.result()

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


_DEFAULT_RECV_PROXY_OPTIONS = {
    "max_concurrency": 1000,
}


def start_recv_proxy(
    cluster: str,
    party: str,
    logging_level: str,
    tls_config=None,
    retry_policy=None,
    actor_config: Optional[fed_config.ProxyActorConfig] = None
):

    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    # NOTE(NKcqx): This is not just addr, but a party dict containing 'address'
    party_addr = cluster[party]
    listen_addr = party_addr.get('listen_addr', None)
    if not listen_addr:
        listen_addr = party_addr['address']

    actor_options = copy.deepcopy(_DEFAULT_RECV_PROXY_OPTIONS)
    if actor_config is not None and actor_config.resource_label is not None:
        actor_options.update({"resources": actor_config.resource_label})

    logger.debug(f"Starting RecvProxyActor with options: {actor_options}")

    recver_proxy_actor = RecverProxyActor.options(
        name=f"RecverProxyActor-{party}", **actor_options
    ).remote(
        listen_addr=listen_addr,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=retry_policy,
    )
    recver_proxy_actor.run_grpc_server.remote()
    timeout = get_cluster_config().cross_silo_timeout
    server_state = ray.get(recver_proxy_actor.is_ready.remote(), timeout=timeout)
    assert server_state[0], server_state[1]
    logger.info("RecverProxy has successfully created.")


_SEND_PROXY_ACTOR = None
_DEFAULT_SEND_PROXY_OPTIONS = {
    "max_concurrency": 1000,
}


def start_send_proxy(
    cluster: Dict,
    party: str,
    logging_level: str,
    tls_config: Dict = None,
    retry_policy=None,
    max_retries=None,
    actor_config: Optional[fed_config.ProxyActorConfig] = None
):
    # Create SendProxyActor
    global _SEND_PROXY_ACTOR

    actor_options = copy.deepcopy(_DEFAULT_SEND_PROXY_OPTIONS)
    if max_retries is not None:
        actor_options.update({
            "max_task_retries": max_retries,
            "max_restarts": 1,
            })
    if actor_config is not None and actor_config.resource_label is not None:
        actor_options.update({"resources": actor_config.resource_label})

    logger.debug(f"Starting SendProxyActor with options: {actor_options}")
    _SEND_PROXY_ACTOR = SendProxyActor.options(
        name="SendProxyActor", **actor_options)

    _SEND_PROXY_ACTOR = _SEND_PROXY_ACTOR.remote(
        cluster=cluster,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=retry_policy,
    )
    timeout = get_cluster_config().cross_silo_timeout
    assert ray.get(_SEND_PROXY_ACTOR.is_ready.remote(), timeout=timeout)
    logger.info("SendProxyActor has successfully created.")


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
    get_global_context()._cleanup_manager.push_to_sending(res)
    return res


def recv(party: str, src_party: str, upstream_seq_id, curr_seq_id):
    assert party, 'Party can not be None.'
    receiver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    return receiver_proxy.get_data.remote(src_party, upstream_seq_id, curr_seq_id)


def ping_others(cluster: Dict[str, Dict], self_party: str, max_retries=3600):
    """Ping other parties until all are ready or timeout."""
    others = [party for party in cluster if not party == self_party]
    tried = 0

    while tried < max_retries and others:
        logger.info(
            f'Try ping {others} at {tried} attemp, up to {max_retries} attemps.'
        )
        tried += 1
        _party_ping_obj = {}  # {$party_name: $ObjectRef}
        # Batch ping all the other parties
        for other in others:
            _party_ping_obj[other] = send(other, b'data', 'ping', 'ping')
        _, _unready = ray.wait(list(_party_ping_obj.values()), timeout=1)

        # Keep the unready party for the next ping.
        others = [
            other for other in others if _party_ping_obj[other] in _unready
        ]
        if others:
            time.sleep(2)
    if others:
        raise RuntimeError(f"Failed to wait for parties: {others} to start, "
                           "abort `fed.init`.")
    return True
