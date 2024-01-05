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
import copy
import json
import logging
import threading
from typing import Dict

import cloudpickle
import grpc

import fed._private.compatible_utils as compatible_utils
import fed.utils as fed_utils
from fed.config import CrossSiloMessageConfig, GrpcCrossSiloMessageConfig
from fed.proxy.barriers import (
    add_two_dim_dict,
    get_from_two_dim_dict,
    key_exists_in_two_dim_dict,
    pop_from_two_dim_dict,
)
from fed.proxy.base_proxy import ReceiverProxy, SenderProxy
from fed.proxy.grpc.grpc_options import _DEFAULT_GRPC_CHANNEL_OPTIONS, _GRPC_SERVICE

if compatible_utils._compare_version_strings(
    fed_utils.get_package_version('protobuf'), '4.0.0'
):
    from fed.grpc.pb4 import fed_pb2 as fed_pb2
    from fed.grpc.pb4 import fed_pb2_grpc as fed_pb2_grpc
else:
    from fed.grpc.pb3 import fed_pb2 as fed_pb2
    from fed.grpc.pb3 import fed_pb2_grpc as fed_pb2_grpc


logger = logging.getLogger(__name__)


def parse_grpc_options(proxy_config: CrossSiloMessageConfig):
    """
    Extract certain fields in `CrossSiloGrpcCommConfig` into the
    "grpc_channel_options". Note that the resulting dict's key
    may not be identical to the config name, but a grpc-supported
    option name.

    Args:
        proxy_config (CrossSiloMessageConfig): The proxy configuration
            from which to extract the gRPC options.

    Returns:
        dict: A dictionary containing the gRPC channel options.
    """
    grpc_channel_options = {}
    if proxy_config is not None:
        # NOTE(NKcqx): `messages_max_size_in_bytes` is a common cross-silo
        # config that should be extracted and filled into proper grpc's
        # channel options.
        # However, `GrpcCrossSiloMessageConfig` provides a more flexible way
        # to configure grpc channel options, i.e. the `grpc_channel_options`
        # field, which may override the `messages_max_size_in_bytes` field.
        if isinstance(proxy_config, CrossSiloMessageConfig):
            if proxy_config.messages_max_size_in_bytes is not None:
                grpc_channel_options.update(
                    {
                        'grpc.max_send_message_length': proxy_config.messages_max_size_in_bytes,
                        'grpc.max_receive_message_length': proxy_config.messages_max_size_in_bytes,
                    }
                )
        if isinstance(proxy_config, GrpcCrossSiloMessageConfig):
            if proxy_config.grpc_channel_options is not None:
                grpc_channel_options.update(proxy_config.grpc_channel_options)
            if proxy_config.grpc_retry_policy is not None:
                grpc_channel_options.update(
                    {
                        'grpc.service_config': json.dumps(
                            {
                                'methodConfig': [
                                    {
                                        'name': [{'service': _GRPC_SERVICE}],
                                        'retryPolicy': proxy_config.grpc_retry_policy,
                                    }
                                ]
                            }
                        ),
                    }
                )

    return grpc_channel_options


class GrpcSenderProxy(SenderProxy):
    def __init__(
        self,
        cluster: Dict,
        party: str,
        job_name: str,
        tls_config: Dict,
        proxy_config: Dict = None,
    ) -> None:
        proxy_config = GrpcCrossSiloMessageConfig.from_dict(proxy_config)
        super().__init__(cluster, party, job_name, tls_config, proxy_config)
        self._grpc_metadata = proxy_config.http_header or {}
        self._grpc_options = copy.deepcopy(_DEFAULT_GRPC_CHANNEL_OPTIONS)
        self._grpc_options.update(parse_grpc_options(self._proxy_config))
        # Mapping the destination party name to the reused client stub.
        self._stubs = {}

    async def send(self, dest_party, data, upstream_seq_id, downstream_seq_id):
        dest_addr = self._addresses[dest_party]
        grpc_metadata, grpc_channel_options = self.get_grpc_config_by_party(dest_party)
        tls_enabled = fed_utils.tls_enabled(self._tls_config)
        if dest_party not in self._stubs:
            if tls_enabled:
                ca_cert, private_key, cert_chain = fed_utils.load_cert_config(
                    self._tls_config
                )
                credentials = grpc.ssl_channel_credentials(
                    certificate_chain=cert_chain,
                    private_key=private_key,
                    root_certificates=ca_cert,
                )
                channel = grpc.aio.secure_channel(
                    dest_addr, credentials, options=grpc_channel_options
                )
            else:
                channel = grpc.aio.insecure_channel(
                    dest_addr, options=grpc_channel_options
                )
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            self._stubs[dest_party] = stub

        timeout = self._proxy_config.timeout_in_ms / 1000
        response = await send_data_grpc(
            data=data,
            stub=self._stubs[dest_party],
            upstream_seq_id=upstream_seq_id,
            downstream_seq_id=downstream_seq_id,
            job_name=self._job_name,
            timeout=timeout,
            metadata=grpc_metadata,
        )
        self.handle_response_error(response)
        return response.result

    def get_grpc_config_by_party(self, dest_party):
        """Overide global config by party specific config"""
        grpc_metadata = self._grpc_metadata
        grpc_options = self._grpc_options

        dest_party_msg_config = self._proxy_config
        if dest_party_msg_config is not None:
            if dest_party_msg_config.http_header is not None:
                dest_party_grpc_metadata = dict(dest_party_msg_config.http_header)
                grpc_metadata = {**grpc_metadata, **dest_party_grpc_metadata}
            dest_party_grpc_options = parse_grpc_options(dest_party_msg_config)
            grpc_options = {**grpc_options, **dest_party_grpc_options}
        return grpc_metadata, fed_utils.dict2tuple(grpc_options)

    async def get_proxy_config(self, dest_party=None):
        if dest_party is None:
            grpc_options = fed_utils.dict2tuple(self._grpc_options)
        else:
            _, grpc_options = self.get_grpc_config_by_party(dest_party)
        proxy_config = self._proxy_config.__dict__
        proxy_config.update({'grpc_options': grpc_options})
        return proxy_config

    def handle_response_error(self, response):
        if response.code == 200:
            return

        if 400 <= response.code < 500:
            # Request error should also be identified as a sending failure,
            # though the request was physically sent.
            logger.warning(
                f"Request was successfully sent but got error response, "
                f"code: {response.code}, message: {response.result}."
            )
            raise RuntimeError(response.result)


async def send_data_grpc(
    data,
    stub,
    upstream_seq_id,
    downstream_seq_id,
    timeout,
    job_name,
    metadata=None,
):
    data = cloudpickle.dumps(data)
    request = fed_pb2.SendDataRequest(
        data=data,
        upstream_seq_id=str(upstream_seq_id),
        downstream_seq_id=str(downstream_seq_id),
        job_name=job_name,
    )
    # Waiting for the reply from downstream.
    response = await stub.SendData(
        request,
        metadata=fed_utils.dict2tuple(metadata),
        timeout=timeout,
    )
    logger.debug(
        f'Received data response from seq_id {downstream_seq_id}, '
        f'code: {response.code}, '
        f'result: {response.result}.'
    )
    return response


class GrpcReceiverProxy(ReceiverProxy):
    def __init__(
        self,
        listen_addr: str,
        party: str,
        job_name: str,
        tls_config: Dict,
        proxy_config: Dict,
    ) -> None:
        proxy_config = GrpcCrossSiloMessageConfig.from_dict(proxy_config)
        super().__init__(listen_addr, party, job_name, tls_config, proxy_config)
        self._grpc_options = copy.deepcopy(_DEFAULT_GRPC_CHANNEL_OPTIONS)
        self._grpc_options.update(parse_grpc_options(self._proxy_config))

        # Flag to see whether grpc server starts
        self._server_ready_future = asyncio.Future()

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
                self._job_name,
                self._server_ready_future,
                self._tls_config,
                fed_utils.dict2tuple(self._grpc_options),
            )
        except RuntimeError as err:
            msg = (
                f'Grpc server failed to listen to port: {port}'
                f' Try another port by setting `listen_addr` into `cluster` config'
                f' when calling `fed.init`. Grpc error msg: {err}'
            )
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

    async def get_proxy_config(self):
        proxy_config = self._proxy_config.__dict__
        proxy_config.update({'grpc_options': fed_utils.dict2tuple(self._grpc_options)})
        return proxy_config


class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, all_events, all_data, party, lock, job_name):
        self._events = all_events
        self._all_data = all_data
        self._party = party
        self._lock = lock
        self._job_name = job_name

    async def SendData(self, request, context):
        job_name = request.job_name
        if job_name != self._job_name:
            logger.warning(
                f"Receive data from job {job_name}, ignore it. "
                f"The reason may be that the ReceiverProxy is listening "
                f"on the same address with that job."
            )
            return fed_pb2.SendDataResponse(
                code=417,
                result=f"JobName mis-match, expected {self._job_name}, got {job_name}.",
            )
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
        return fed_pb2.SendDataResponse(code=200, result="OK")


async def _run_grpc_server(
    port,
    event,
    all_data,
    party,
    lock,
    job_name,
    server_ready_future,
    tls_config=None,
    grpc_options=None,
):
    logger.info(f"ReceiverProxy binding port {port}, options: {grpc_options}...")
    server = grpc.aio.server(options=grpc_options)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        SendDataService(event, all_data, party, lock, job_name), server
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
