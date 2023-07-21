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

import logging
import time
import copy
from typing import Dict, Optional

import ray

import fed.config as fed_config
from fed.config import get_job_config
from fed.proxy.base_proxy import SenderProxy, ReceiverProxy
from fed.utils import setup_logger
from fed._private import constants
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


@ray.remote
class SenderProxyActor:
    def __init__(
        self,
        addresses: Dict,
        party: str,
        tls_config: Dict = None,
        logging_level: str = None,
        proxy_cls=None
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
            party_val=party,
        )

        self._stats = {"send_op_count": 0}
        self._addresses = addresses
        self._party = party
        self._tls_config = tls_config
        job_config = fed_config.get_job_config()
        cross_silo_message_config = job_config.cross_silo_message_config
        self._proxy_instance: SenderProxy = proxy_cls(
            addresses, party, tls_config, cross_silo_message_config)

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
                dest_party, data, upstream_seq_id, downstream_seq_id)
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
        logging_level: str,
        tls_config=None,
        proxy_cls=None,
    ):
        setup_logger(
            logging_level=logging_level,
            logging_format=constants.RAYFED_LOG_FMT,
            date_format=constants.RAYFED_DATE_FMT,
            party_val=party,
        )
        self._stats = {"receive_op_count": 0}
        self._listening_address = listening_address
        self._party = party
        self._tls_config = tls_config
        job_config = fed_config.get_job_config()
        cross_silo_message_config = job_config.cross_silo_message_config
        self._proxy_instance: ReceiverProxy = proxy_cls(
            listening_address, party, tls_config, cross_silo_message_config)

    async def start(self):
        await self._proxy_instance.start()

    async def is_ready(self):
        res = await self._proxy_instance.is_ready()
        return res

    async def get_data(self, src_party, upstream_seq_id, curr_seq_id):
        self._stats["receive_op_count"] += 1
        data = await self._proxy_instance.get_data(
            src_party, upstream_seq_id, curr_seq_id)
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
    proxy_config: Optional[fed_config.CrossSiloMessageConfig] = None
):

    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    # NOTE(NKcqx): This is not just addr, but a party dict containing 'address'
    party_addr = addresses[party]
    listening_address = proxy_config.listening_address
    if not listening_address:
        listening_address = party_addr

    actor_options = copy.deepcopy(_DEFAULT_RECEIVER_PROXY_OPTIONS)
    if proxy_config is not None and proxy_config.recv_resource_label is not None:
        actor_options.update({"resources": proxy_config.recv_resource_label})

    logger.debug(f"Starting ReceiverProxyActor with options: {actor_options}")

    receiver_proxy_actor = ReceiverProxyActor.options(
        name=f"ReceiverProxyActor-{party}", **actor_options
    ).remote(
        listening_address=listening_address,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        proxy_cls=proxy_cls
    )
    receiver_proxy_actor.start.remote()
    timeout = proxy_config.timeout_in_ms / 1000 if proxy_config is not None else 60
    server_state = ray.get(receiver_proxy_actor.is_ready.remote(), timeout=timeout)
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
    proxy_config: Optional[fed_config.CrossSiloMessageConfig] = None
):
    # Create SenderProxyActor
    global _SENDER_PROXY_ACTOR

    actor_options = copy.deepcopy(_DEFAULT_SENDER_PROXY_OPTIONS)
    if proxy_config and proxy_config.proxy_max_restarts:
        actor_options.update({
            "max_task_retries": proxy_config.proxy_max_restarts,
            "max_restarts": 1,
            })
    if proxy_config and proxy_config.send_resource_label:
        actor_options.update({"resources": proxy_config.send_resource_label})

    logger.debug(f"Starting SenderProxyActor with options: {actor_options}")
    _SENDER_PROXY_ACTOR = SenderProxyActor.options(
        name="SenderProxyActor", **actor_options)

    _SENDER_PROXY_ACTOR = _SENDER_PROXY_ACTOR.remote(
        addresses=addresses,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        proxy_cls=proxy_cls
    )
    timeout = get_job_config().cross_silo_message_config.timeout_in_ms / 1000
    assert ray.get(_SENDER_PROXY_ACTOR.is_ready.remote(), timeout=timeout)
    logger.info("SenderProxyActor has successfully created.")


def send(
    dest_party,
    data,
    upstream_seq_id,
    downstream_seq_id,
):
    sender_proxy = ray.get_actor("SenderProxyActor")
    res = sender_proxy.send.remote(
        dest_party=dest_party,
        data=data,
        upstream_seq_id=upstream_seq_id,
        downstream_seq_id=downstream_seq_id,
    )
    get_global_context().get_cleanup_manager().push_to_sending(res)
    return res


def recv(party: str, src_party: str, upstream_seq_id, curr_seq_id):
    assert party, 'Party can not be None.'
    receiver_proxy = ray.get_actor(f"ReceiverProxyActor-{party}")
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
