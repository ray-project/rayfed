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

import copy
import logging
import time
from typing import Any, Dict

import ray

import fed.config as fed_config
from fed.exceptions import FedRemoteError
from fed._private import constants
from fed._private.global_context import get_global_context
from fed.proxy.base_proxy import ReceiverProxy, SenderProxy, SenderReceiverProxy
from fed.utils import setup_logger

logger = logging.getLogger(__name__)

_SENDER_PROXY_ACTOR_NAME = 'SenderProxyActor'
_RECEIVER_PROXY_ACTOR_NAME = 'ReceiverProxyActor'


def sender_proxy_actor_name() -> str:
    global _SENDER_PROXY_ACTOR_NAME
    return _SENDER_PROXY_ACTOR_NAME


def set_sender_proxy_actor_name(name: str):
    global _SENDER_PROXY_ACTOR_NAME
    _SENDER_PROXY_ACTOR_NAME = name


def receiver_proxy_actor_name() -> str:
    global _RECEIVER_PROXY_ACTOR_NAME
    return _RECEIVER_PROXY_ACTOR_NAME


def set_receiver_proxy_actor_name(name: str):
    global _RECEIVER_PROXY_ACTOR_NAME
    _RECEIVER_PROXY_ACTOR_NAME = name


def set_proxy_actor_name(
    job_name: str, use_global_proxy: bool, sender_recvr_proxy: bool = False
):
    """
    Generate the name of the proxy actor.

    Args:
        job_name: The name of the job, used for actor name's postfix
        use_global_proxy: Whether
            to use a single proxy actor or not. If True, the name of the proxy
            actor will be the default global name, otherwise the name will be
            added with a postfix.
        sender_recvr_proxy: Whether to use the sender-receiver proxy actor or
            not. If True, since there's only one proxy actor, make two actor name
            the same.
    """
    sender_actor_name = (
        constants.RAYFED_DEFAULT_SENDER_PROXY_ACTOR_NAME
        if not sender_recvr_proxy
        else constants.RAYFED_DEFAULT_SENDER_RECEIVER_PROXY_ACTOR_NAME
    )
    receiver_actor_name = (
        constants.RAYFED_DEFAULT_RECEIVER_PROXY_ACTOR_NAME
        if not sender_recvr_proxy
        else constants.RAYFED_DEFAULT_SENDER_RECEIVER_PROXY_ACTOR_NAME
    )
    if not use_global_proxy:
        sender_actor_name = f"{sender_actor_name}_{job_name}"
        receiver_actor_name = f"{receiver_actor_name}_{job_name}"
    set_sender_proxy_actor_name(sender_actor_name)
    set_receiver_proxy_actor_name(receiver_actor_name)


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


@ray.remote
class SenderProxyActor:
    def __init__(
        self,
        addresses: Dict,
        party: str,
        job_name: str,
        tls_config: Dict = None,
        logging_level: str = None,
        proxy_cls=None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
            party_val=party,
            job_name=job_name,
        )

        self._stats = {"send_op_count": 0}
        self._addresses = addresses
        self._party = party
        self._job_name = job_name
        self._tls_config = tls_config
        job_config = fed_config.get_job_config(job_name)
        cross_silo_comm_config = job_config.cross_silo_comm_config_dict
        self._proxy_instance: SenderProxy = proxy_cls(
            addresses, party, job_name, tls_config, cross_silo_comm_config
        )

    async def is_ready(self):
        res = await self._proxy_instance.is_ready()
        return res

    async def send(
        self,
        dest_party,
        data,
        upstream_seq_id,
        downstream_seq_id,
    ):
        self._stats["send_op_count"] += 1
        assert (
            dest_party in self._addresses
        ), f'Failed to find {dest_party} in addresses {self._addresses}.'
        send_log_msg = (
            f'send data to seq_id {downstream_seq_id} of {dest_party} '
            f'from {upstream_seq_id}'
        )
        logger.debug(
            f'Sending {send_log_msg} with{"out" if not self._tls_config else ""}'
            ' credentials.'
        )
        try:
            response = await self._proxy_instance.send(
                dest_party, data, upstream_seq_id, downstream_seq_id
            )
        except Exception as e:
            logger.error(f'Failed to {send_log_msg}, error: {e}')
            return False
        logger.debug(f"Succeeded to send {send_log_msg}. Response is {response}")
        return True  # True indicates it's sent successfully.

    async def _get_stats(self):
        return self._stats

    async def _get_addresses_info(self):
        return self._addresses

    async def _get_proxy_config(self, dest_party=None):
        return await self._proxy_instance.get_proxy_config(dest_party)


@ray.remote
class ReceiverProxyActor:
    def __init__(
        self,
        listening_address: str,
        party: str,
        job_name: str,
        logging_level: str,
        tls_config=None,
        proxy_cls=None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
            party_val=party,
            job_name=job_name,
        )
        self._stats = {"receive_op_count": 0}
        self._listening_address = listening_address
        self._party = party
        self._job_name = job_name
        self._tls_config = tls_config
        job_config = fed_config.get_job_config(job_name)
        cross_silo_comm_config = job_config.cross_silo_comm_config_dict
        self._proxy_instance: ReceiverProxy = proxy_cls(
            listening_address, party, job_name, tls_config, cross_silo_comm_config
        )

    async def start(self):
        await self._proxy_instance.start()

    async def is_ready(self):
        res = await self._proxy_instance.is_ready()
        return res

    async def get_data(self, src_party, upstream_seq_id, curr_seq_id):
        self._stats["receive_op_count"] += 1
        data = await self._proxy_instance.get_data(
            src_party, upstream_seq_id, curr_seq_id
        )
        if isinstance(data, FedRemoteError):
            logger.debug(
                f"Receiving exception: {type(data)}, {data} from {src_party}, "
                f"upstream_seq_id: {upstream_seq_id}, "
                f"curr_seq_id: {curr_seq_id}. Re-raise it."
            )
            raise data
        return data

    async def _get_stats(self):
        return self._stats

    async def _get_proxy_config(self):
        return await self._proxy_instance.get_proxy_config()


_DEFAULT_RECEIVER_PROXY_OPTIONS = {
    "max_concurrency": 1000,
}


def _start_receiver_proxy(
    addresses: str,
    party: str,
    logging_level: str,
    tls_config=None,
    proxy_cls=None,
    proxy_config: Dict = None,
    ready_timeout_second: int = 60,
):
    actor_options = copy.deepcopy(_DEFAULT_RECEIVER_PROXY_OPTIONS)
    proxy_config = fed_config.CrossSiloMessageConfig.from_dict(proxy_config)
    if proxy_config.recv_resource_label is not None:
        actor_options.update({"resources": proxy_config.recv_resource_label})
    if proxy_config.max_concurrency:
        actor_options.update({"max_concurrency": proxy_config.max_concurrency})
    actor_options.update({"name": receiver_proxy_actor_name()})

    logger.debug(f"Starting ReceiverProxyActor with options: {actor_options}")
    job_name = get_global_context().get_job_name()

    receiver_proxy_actor = ReceiverProxyActor.options(**actor_options).remote(
        listening_address=addresses[party],
        party=party,
        job_name=job_name,
        tls_config=tls_config,
        logging_level=logging_level,
        proxy_cls=proxy_cls,
    )
    receiver_proxy_actor.start.remote()
    server_state = ray.get(
        receiver_proxy_actor.is_ready.remote(), timeout=ready_timeout_second
    )
    assert server_state[0], server_state[1]
    logger.info("Succeeded to create receiver proxy actor.")


_SENDER_PROXY_ACTOR = None
_DEFAULT_SENDER_PROXY_OPTIONS = {
    "max_concurrency": 1000,
}


def _start_sender_proxy(
    addresses: Dict,
    party: str,
    logging_level: str,
    tls_config: Dict = None,
    proxy_cls=None,
    proxy_config: Dict = None,
    ready_timeout_second: int = 60,
):
    actor_options = copy.deepcopy(_DEFAULT_SENDER_PROXY_OPTIONS)
    proxy_config = fed_config.GrpcCrossSiloMessageConfig.from_dict(proxy_config)
    if proxy_config.proxy_max_restarts:
        actor_options.update(
            {
                "max_task_retries": proxy_config.proxy_max_restarts,
                "max_restarts": 1,
            }
        )
    if proxy_config.send_resource_label:
        actor_options.update({"resources": proxy_config.send_resource_label})
    if proxy_config.max_concurrency:
        actor_options.update({"max_concurrency": proxy_config.max_concurrency})

    job_name = get_global_context().get_job_name()
    actor_options.update({"name": sender_proxy_actor_name()})

    logger.debug(f"Starting SenderProxyActor with options: {actor_options}")
    global _SENDER_PROXY_ACTOR

    _SENDER_PROXY_ACTOR = SenderProxyActor.options(**actor_options)

    _SENDER_PROXY_ACTOR = _SENDER_PROXY_ACTOR.remote(
        addresses=addresses,
        party=party,
        job_name=job_name,
        tls_config=tls_config,
        logging_level=logging_level,
        proxy_cls=proxy_cls,
    )
    assert ray.get(_SENDER_PROXY_ACTOR.is_ready.remote(), timeout=ready_timeout_second)
    logger.info("SenderProxyActor has successfully created.")


_SENDER_RECEIVER_PROXY_ACTOR = None
_DEFAULT_SENDER_RECEIVER_PROXY_OPTIONS = {
    "max_concurrency": 1,
}


@ray.remote
class SenderReceiverProxyActor:
    def __init__(
        self,
        addresses: Dict,
        party: str,
        job_name: str,
        tls_config: Dict = None,
        logging_level: str = None,
        proxy_cls: SenderReceiverProxy = None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
            party_val=party,
            job_name=job_name,
        )

        self._stats = {'send_op_count': 0, 'receive_op_count': 0}
        self._addresses = addresses
        self._party = party
        self._tls_config = tls_config
        job_config = fed_config.get_job_config(job_name=job_name)
        cross_silo_comm_config = job_config.cross_silo_comm_config_dict
        self._proxy_instance = proxy_cls(
            addresses, party, tls_config, cross_silo_comm_config
        )

    def is_ready(self):
        return self._proxy_instance.is_ready()

    def start(self):
        self._proxy_instance.start()

    def get_data(self, src_party, upstream_seq_id, curr_seq_id):
        self._stats["receive_op_count"] += 1
        data = self._proxy_instance.get_data(src_party, upstream_seq_id, curr_seq_id)
        return data

    def send(
        self,
        dest_party,
        data,
        upstream_seq_id,
        downstream_seq_id,
    ):
        self._stats["send_op_count"] += 1
        assert (
            dest_party in self._addresses
        ), f'Failed to find {dest_party} in cluster {self._addresses}.'
        send_log_msg = (
            f'send data to seq_id {downstream_seq_id} of {dest_party} '
            f'from {upstream_seq_id}'
        )
        logger.debug(
            f'Sending {send_log_msg} with{"out" if not self._tls_config else ""}'
            ' credentials.'
        )
        try:
            response = self._proxy_instance.send(
                dest_party, data, upstream_seq_id, downstream_seq_id
            )
        except Exception as e:
            logger.error(f'Failed to {send_log_msg}, error: {e}')
            return False
        logger.debug(f"Succeeded to {send_log_msg}. Response is {response}")
        return True  # True indicates it's sent successfully.

    def _get_stats(self):
        return self._stats

    def _get_proxy_config(self, dest_party=None):
        return self._proxy_instance.get_proxy_config(dest_party)


def _start_sender_receiver_proxy(
    addresses: str,
    party: str,
    logging_level: str,
    tls_config=None,
    proxy_cls=None,
    proxy_config: Dict = None,
    ready_timeout_second: int = 60,
):
    global _DEFAULT_SENDER_RECEIVER_PROXY_OPTIONS
    actor_options = copy.deepcopy(_DEFAULT_SENDER_RECEIVER_PROXY_OPTIONS)
    proxy_config = fed_config.CrossSiloMessageConfig.from_dict(proxy_config)
    if proxy_config.proxy_max_restarts:
        actor_options.update(
            {
                "max_task_retries": proxy_config.proxy_max_restarts,
                "max_restarts": 1,
            }
        )
    if proxy_config.max_concurrency:
        actor_options.update({"max_concurrency": proxy_config.max_concurrency})

    # NOTE(NKcqx): sender & receiver have the same name
    actor_options.update({"name": receiver_proxy_actor_name()})
    logger.debug(f"Starting ReceiverProxyActor with options: {actor_options}")

    job_name = get_global_context().get_job_name()
    global _SENDER_RECEIVER_PROXY_ACTOR

    _SENDER_RECEIVER_PROXY_ACTOR = SenderReceiverProxyActor.options(
        **actor_options
    ).remote(
        addresses=addresses,
        party=party,
        job_name=job_name,
        tls_config=tls_config,
        logging_level=logging_level,
        proxy_cls=proxy_cls,
    )
    _SENDER_RECEIVER_PROXY_ACTOR.start.remote()
    server_state = ray.get(
        _SENDER_RECEIVER_PROXY_ACTOR.is_ready.remote(), timeout=ready_timeout_second
    )
    assert server_state[0], server_state[1]
    logger.info("Succeeded to create receiver proxy actor.")


def send(
    dest_party: str,
    data: Any,
    upstream_seq_id: int,
    downstream_seq_id: int,
    is_error: bool = False,
    check_sending: bool = True,
):
    """
    Args:
        is_error: Whether the `data` is an error object or not. Default is False.
            If True, the data will be sent to the error message queue.
        check_sending: Whether to check the data sending. If true, the data will be
            checked in the sending check loop.
    """
    sender_proxy = ray.get_actor(sender_proxy_actor_name())
    res = sender_proxy.send.remote(
        dest_party=dest_party,
        data=data,
        upstream_seq_id=upstream_seq_id,
        downstream_seq_id=downstream_seq_id,
    )
    if check_sending:
        get_global_context().get_cleanup_manager().push_to_sending(
            res, dest_party, upstream_seq_id, downstream_seq_id, is_error
        )
    return res


def recv(party: str, src_party: str, upstream_seq_id: int, curr_seq_id: int):
    assert party, 'Party can not be None.'
    receiver_proxy = ray.get_actor(receiver_proxy_actor_name())
    return receiver_proxy.get_data.remote(src_party, upstream_seq_id, curr_seq_id)


def ping_others(addresses: Dict[str, Dict], self_party: str, max_retries=3600):
    """Ping other parties until all are ready or timeout."""
    others = [party for party in addresses if not party == self_party]
    tried = 0

    while tried < max_retries and others:
        logger.info(
            f'Try ping {others} at {tried} attemp, up to {max_retries} attemps.'
        )
        tried += 1
        _party_ping_obj = {}  # {$party_name: $ObjectRef}
        # Batch ping all the other parties
        for other in others:
            _party_ping_obj[other] = send(
                other, b'data', 'ping', 'ping', check_sending=False
            )
        _, _unready = ray.wait(list(_party_ping_obj.values()), timeout=1)

        # Keep the unready party for the next ping.
        others = [other for other in others if _party_ping_obj[other] in _unready]
        if others:
            time.sleep(2)
    if others:
        raise RuntimeError(
            f"Failed to wait for parties: {others} to start, " "abort `fed.init`."
        )
    return True
