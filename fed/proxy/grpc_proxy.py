import asyncio
import cloudpickle
import grpc
import logging
import threading
from typing import Dict


import fed.utils as fed_utils

from fed.config import CrossSiloCommConfig, CrossSiloGrpcCommConfig
from fed._private.grpc_options import get_grpc_options, set_max_message_length
from fed.proxy.barriers import (
    add_two_dim_dict,
    get_from_two_dim_dict,
    pop_from_two_dim_dict,
    key_exists_in_two_dim_dict,
    SendProxy,
    RecvProxy
)
from fed.grpc import fed_pb2, fed_pb2_grpc


logger = logging.getLogger(__name__)


class GrpcSendProxy(SendProxy):
    def __init__(
            self,
            cluster: Dict,
            party: str,
            tls_config: Dict,
            proxy_config=None
    ) -> None:
        super().__init__(cluster, party, tls_config, proxy_config)
        self._grpc_metadata = proxy_config.http_header
        set_max_message_length(proxy_config.messages_max_size_in_bytes)
        self._retry_policy = None
        if isinstance(proxy_config, CrossSiloGrpcCommConfig):
            self._retry_policy = proxy_config.grpc_retry_policy
        # Mapping the destination party name to the reused client stub.
        self._stubs = {}

    async def send(
            self,
            dest_party,
            data,
            upstream_seq_id,
            downstream_seq_id):
        dest_addr = self._cluster[dest_party]['address']
        dest_party_grpc_config = self.setup_grpc_config(dest_party)
        tls_enabled = fed_utils.tls_enabled(self._tls_config)
        grpc_options = dest_party_grpc_config['grpc_options']
        grpc_options = get_grpc_options(retry_policy=self._retry_policy) if \
            grpc_options is None else fed_utils.dict2tuple(grpc_options)
        if dest_party not in self._stubs:
            if tls_enabled:
                ca_cert, private_key, cert_chain = fed_utils.load_cert_config(
                    self._tls_config)
                credentials = grpc.ssl_channel_credentials(
                    certificate_chain=cert_chain,
                    private_key=private_key,
                    root_certificates=ca_cert,
                )
                channel = grpc.aio.secure_channel(
                    dest_addr, credentials, options=grpc_options)
            else:
                channel = grpc.aio.insecure_channel(dest_addr, options=grpc_options)
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            self._stubs[dest_party] = stub

        timeout = self._proxy_config.timeout_in_seconds
        response = await send_data_grpc(
            data=data,
            stub=self._stubs[dest_party],
            upstream_seq_id=upstream_seq_id,
            downstream_seq_id=downstream_seq_id,
            timeout=timeout,
            metadata=dest_party_grpc_config['grpc_metadata'],
        )
        return response

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

        global_grpc_options = dict(get_grpc_options(self._retry_policy))
        dest_party_grpc_options = dict(
            self._cluster[dest_party].get('grpc_options', {})
        )
        dest_party_grpc_config['grpc_options'] = {
            **global_grpc_options, **dest_party_grpc_options}
        return dest_party_grpc_config

    async def _get_grpc_options(self):
        return get_grpc_options()


async def send_data_grpc(
    data,
    stub,
    upstream_seq_id,
    downstream_seq_id,
    timeout,
    metadata=None,
):
    data = cloudpickle.dumps(data)
    request = fed_pb2.SendDataRequest(
        data=data,
        upstream_seq_id=str(upstream_seq_id),
        downstream_seq_id=str(downstream_seq_id),
    )
    # Waiting for the reply from downstream.
    response = await stub.SendData(
        request,
        metadata=fed_utils.dict2tuple(metadata),
        timeout=timeout,
    )
    logger.debug(
        f'Received data response from seq_id {downstream_seq_id}, '
        f'result: {response.result}.'
    )
    return response.result


class GrpcRecvProxy(RecvProxy):
    def __init__(
            self,
            listen_addr: str,
            party: str,
            tls_config: Dict,
            proxy_config: CrossSiloCommConfig
    ) -> None:
        super().__init__(listen_addr, party, tls_config, proxy_config)
        set_max_message_length(proxy_config.messages_max_size_in_bytes)
        # Flag to see whether grpc server starts
        self._server_ready_future = asyncio.Future()
        self._retry_policy = None
        if isinstance(proxy_config, CrossSiloGrpcCommConfig):
            self._retry_policy = proxy_config.grpc_retry_policy

        # All events for grpc waitting usage.
        self._events = {}  # map from (upstream_seq_id, downstream_seq_id) to event
        self._all_data = {}  # map from (upstream_seq_id, downstream_seq_id) to data
        self._lock = threading.Lock()

    async def start(self):
        port = self._listen_addr[self._listen_addr.index(':') + 1 :]
        try:
            await _run_grpc_server(
                port,
                self._events,
                self._all_data,
                self._party,
                self._lock,
                self._server_ready_future,
                self._tls_config,
                get_grpc_options(self._retry_policy),
            )
        except RuntimeError as err:
            msg = f'Grpc server failed to listen to port: {port}' \
                  f' Try another port by setting `listen_addr` into `cluster` config' \
                  f' when calling `fed.init`. Grpc error msg: {err}'
            self._server_ready_future.set_result((False, msg))

    async def is_ready(self):
        await self._server_ready_future
        res = self._server_ready_future.result()
        return res

    async def get_data(self, src_party, upstream_seq_id, curr_seq_id):
        data_log_msg = f"data for {curr_seq_id} from {upstream_seq_id} of {src_party}"
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
        logger.debug(f"Waited {data_log_msg}.")
        with self._lock:
            data = pop_from_two_dim_dict(self._all_data, upstream_seq_id, curr_seq_id)
            pop_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)

        # NOTE(qwang): This is used to avoid the conflict with pickle5 in Ray.
        import fed._private.serialization_utils as fed_ser_utils
        fed_ser_utils._apply_loads_function_with_whitelist()
        return cloudpickle.loads(data)

    async def _get_grpc_options(self):
        return get_grpc_options()


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
