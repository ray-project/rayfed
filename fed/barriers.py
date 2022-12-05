import asyncio
import logging
import threading

import cloudpickle
import grpc
import ray

import fed.utils as fed_utils
from fed._private.constants import GRPC_OPTIONS
from fed.cleanup import push_to_sending
from fed.grpc import fed_pb2, fed_pb2_grpc
import fed._private.serialization_utils


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
            f"[{self._party}] Received a grpc data request from {upstream_seq_id} to {downstream_seq_id}."
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
        logger.debug(f"[{self._party}] Event set for {upstream_seq_id}")
        return fed_pb2.SendDataResponse(result="OK")


async def _run_grpc_server(port, event, all_data, party, lock, tls_config=None):
    server = grpc.aio.server(options=GRPC_OPTIONS)
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(
        SendDataService(event, all_data, party, lock), server
    )

    tls_enabled = fed_utils.tls_enabled(tls_config)
    if tls_enabled:
        ca_cert, private_key, cert_chain = fed_utils.load_server_certs(tls_config)
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
        f"Successfully start Grpc service with{'out' if not tls_enabled else ''} credentials."
    )
    await server.wait_for_termination()


async def send_data_grpc(
    dest, data, upstream_seq_id, downstream_seq_id, tls_config=None, node_party=None
):
    tls_enabled = fed_utils.tls_enabled(tls_config)
    if tls_enabled:
        ca_cert, private_key, cert_chain = fed_utils.load_client_certs(
            tls_config, target_party=node_party
        )
        credentials = grpc.ssl_channel_credentials(
            certificate_chain=cert_chain,
            private_key=private_key,
            root_certificates=ca_cert,
        )

        async with grpc.aio.secure_channel(
            dest,
            credentials,
            options=GRPC_OPTIONS
            + [
                # ('grpc.ssl_target_name_override', "rayfed"),
                # ("grpc.default_authority", "rayfed"),
            ],
        ) as channel:
            await channel.channel_ready()
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            data = cloudpickle.dumps(data)
            request = fed_pb2.SendDataRequest(
                data=data,
                upstream_seq_id=str(upstream_seq_id),
                downstream_seq_id=str(downstream_seq_id),
            )
            # wait for downstream's reply
            response = await stub.SendData(request, timeout=60)
            logger.debug(
                f"Received data response from seq_id {downstream_seq_id} result: {response.result}."
            )
            return response.result
    else:
        async with grpc.aio.insecure_channel(dest, options=GRPC_OPTIONS) as channel:
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            data = cloudpickle.dumps(data)
            request = fed_pb2.SendDataRequest(
                data=data,
                upstream_seq_id=str(upstream_seq_id),
                downstream_seq_id=str(downstream_seq_id),
            )
            # wait for downstream's reply
            response = await stub.SendData(request, timeout=60)
            logger.debug(
                f"Received data response from seq_id {downstream_seq_id} result: {response.result}."
            )
            return response.result


@ray.remote
class SendProxyActor:
    async def __init__(self, party):
        self._party = party

    async def is_ready(self):
        return True

    async def send(
        self,
        dest,
        data,
        upstream_seq_id,
        downstream_seq_id,
        tls_config=None,
        node_party=None,
    ):
        logger.debug(
            f"[{self._party}] Sending data to seq_id {downstream_seq_id} from {upstream_seq_id}"
        )
        response = await send_data_grpc(
            dest=dest,
            data=data,
            upstream_seq_id=upstream_seq_id,
            downstream_seq_id=downstream_seq_id,
            tls_config=tls_config,
            node_party=node_party,
        )
        logger.debug(f"Sent. Response is {response}")
        return True  # True indicates it's sent successfully.


@ray.remote
class RecverProxyActor:
    async def __init__(self, listen_addr: str, party: str, tls_config=None):
        self._listen_addr = listen_addr
        self._party = party
        self._tls_config = tls_config

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
        )

    async def is_ready(self):
        return True

    async def get_data(self, upstream_seq_id, curr_seq_id):
        logger.debug(
            f"[{self._party}] Getting data for {curr_seq_id} from {upstream_seq_id}"
        )
        with self._lock:
            if not key_exists_in_two_dim_dict(
                self._events, upstream_seq_id, curr_seq_id
            ):
                add_two_dim_dict(
                    self._events, upstream_seq_id, curr_seq_id, asyncio.Event()
                )

        curr_event = get_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)
        await curr_event.wait()
        logging.debug(f"[{self._party}] Waited for {curr_seq_id}.")
        with self._lock:
            data = pop_from_two_dim_dict(self._all_data, upstream_seq_id, curr_seq_id)
            pop_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)
        return cloudpickle.loads(data)


def start_recv_proxy(listen_addr, party, tls_config=None):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    recver_proxy_actor = RecverProxyActor.options(
        name=f"RecverProxyActor-{party}", max_concurrency=1000
    ).remote(listen_addr, party, tls_config)
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())
    logger.info("RecverProxy was successfully created.")


_SEND_PROXY_ACTOR = None


def start_send_proxy(party):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    global _SEND_PROXY_ACTOR
    _SEND_PROXY_ACTOR = SendProxyActor.options(
        name="SendProxyActor", max_concurrency=1000
    ).remote(party)
    assert ray.get(_SEND_PROXY_ACTOR.is_ready.remote())
    logger.info("SendProxy was successfully created.")


def send(
    dest,
    data,
    upstream_seq_id,
    downstream_seq_id,
    tls_config=None,
    node_party=None,
):
    send_proxy = ray.get_actor("SendProxyActor")
    res = send_proxy.send.remote(
        dest=dest,
        data=data,
        upstream_seq_id=upstream_seq_id,
        downstream_seq_id=downstream_seq_id,
        tls_config=tls_config,
        node_party=node_party,
    )
    push_to_sending(res)
    return res


def recv(party: str, upstream_seq_id, curr_seq_id):
    assert party, 'Party can not be None.'
    receiver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    return receiver_proxy.get_data.remote(upstream_seq_id, curr_seq_id)
