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

import functools
import inspect
import logging
from typing import Any, Dict, List, Union

import cloudpickle
import ray

import fed._private.compatible_utils as compatible_utils
import fed.config as fed_config
import fed.utils as fed_utils
from fed._private import constants
from fed._private.fed_actor import FedActorHandle
from fed._private.fed_call_holder import FedCallHolder
from fed._private.global_context import get_global_context
from fed.barriers import ping_others, recv, send, start_recv_proxy, start_send_proxy
from fed.cleanup import set_exit_on_failure_sending, wait_sending
from fed.fed_object import FedObject
from fed.utils import is_ray_object_refs, setup_logger

logger = logging.getLogger(__name__)


def init(
    address: str = None,
    cluster: Dict = None,
    party: str = None,
    tls_config: Dict = None,
    logging_level: str = 'info',
    cross_silo_grpc_retry_policy: Dict = None,
    cross_silo_send_max_retries: int = None,
    cross_silo_serializing_allowed_list: Dict = None,
    exit_on_failure_cross_silo_sending: bool = False,
    cross_silo_messages_max_size_in_bytes: int = None,
    cross_silo_timeout_in_seconds: int = 60,
    enable_waiting_for_other_parties_ready: bool = False,
    grpc_metadata: Dict = None,
    **kwargs,
):
    """
    Initialize a RayFed client. It connects an exist or starts a new local
        cluster.

    Args:
        address: optional; the address of Ray Cluster, same as `address` of
            `ray.init`.
        cluster: optional; a dict describes the cluster config. E.g.

            .. code:: python
                {
                    'alice': {
                        # The address for other parties.
                        'address': '127.0.0.1:10001',
                        # (Optional) the listen address, the `address` will be
                        # used if not provided.
                        'listen_addr': '0.0.0.0:10001'
                    },
                    'bob': {
                        # The address for other parties.
                        'address': '127.0.0.1:10002',
                        # (Optional) the listen address, the `address` will be
                        # used if not provided.
                        'listen_addr': '0.0.0.0:10002'
                    },
                    'carol': {
                        # The address for other parties.
                        'address': '127.0.0.1:10003',
                        # (Optional) the listen address, the `address` will be
                        # used if not provided.
                        'listen_addr': '0.0.0.0:10003'
                    },
                }
        party: optional; self party.
        tls_config: optional; a dict describes the tls config. E.g.
            For alice,

            .. code:: python
                {
                    "ca_cert": "root ca cert of other parties.",
                    "cert": "alice's server cert",
                    "key": "alice's server cert key",
                }

            For bob,

            .. code:: python
                {
                    "ca_cert": "root ca cert of other parties.",
                    "cert": "bob's server cert",
                    "key": "bob's server cert key",
                }

        logging_level: optional; the logging level, could be `debug`, `info`,
            `warning`, `error`, `critical`, not case sensititive.
        cross_silo_grpc_retry_policy: a dict descibes the retry policy for
            cross silo rpc call. If None, the following default retry policy
            will be used. More details please refer to
            `retry-policy <https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy>`_. # noqa

            .. code:: python
                {
                    "maxAttempts": 4,
                    "initialBackoff": "0.1s",
                    "maxBackoff": "1s",
                    "backoffMultiplier": 2,
                    "retryableStatusCodes": [
                        "UNAVAILABLE"
                    ]
                }
        cross_silo_send_max_retries: the max retries for sending data cross silo.
        cross_silo_serializing_allowed_list: The package or class list allowed for
            serializing(deserializating) cross silos. It's used for avoiding pickle
            deserializing execution attack when crossing solis.
        exit_on_failure_cross_silo_sending: whether exit when failure on
            cross-silo sending. If True, a SIGTERM will be signaled to self
            if failed to sending cross-silo data.
        cross_silo_messages_max_size_in_bytes: The maximum length in bytes of
            cross-silo messages.
            If None, the default value of 500 MB is specified.
        cross_silo_timeout_in_seconds: The timeout in seconds of a cross-silo RPC call.
            It's 60 by default.
        enable_waiting_for_other_parties_ready: ping other parties until they
            are all ready if True.
        grpc_metadata: optional; The metadata sent with the grpc request. This won't override
            basic tcp headers, such as `user-agent`, but aggregate them together.
        kwargs: the args for ray.init().

    Examples:
        >>> import fed
        >>> cluster = {
        >>>    'alice': {'address': '127.0.0.1:10001'},
        >>>    'bob': {'address': '127.0.0.1:10002'},
        >>>    'carol': {'address': '127.0.0.1:10003'},
        >>> }
        >>> # Start as alice.
        >>> fed.init(address='local', cluster=cluster, self_party='alice')
    """
    assert cluster, "Cluster should be provided."
    assert party, "Party should be provided."
    assert party in cluster, f"Party {party} is not in cluster {cluster}."

    compatible_utils.init_ray(address=address, **kwargs)
    tls_config = {} if tls_config is None else tls_config
    if tls_config:
        assert (
            'cert' in tls_config and 'key' in tls_config
        ), 'Cert or key are not in tls_config.'
    # A Ray private accessing, should be replaced in public API.

    compatible_utils._init_internal_kv()
    compatible_utils.kv.initialize()

    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: cluster,
        constants.KEY_OF_CURRENT_PARTY_NAME: party,
        constants.KEY_OF_TLS_CONFIG: tls_config,
        constants.KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST:
            cross_silo_serializing_allowed_list,
        constants.KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES:
            cross_silo_messages_max_size_in_bytes,
        constants.KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS: cross_silo_timeout_in_seconds,
    }

    job_config = {
       constants.KEY_OF_GRPC_METADATA : grpc_metadata,
    }
    compatible_utils.kv.put(constants.KEY_OF_CLUSTER_CONFIG,
                            cloudpickle.dumps(cluster_config))
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG, cloudpickle.dumps(job_config))
    # Set logger.
    # Note(NKcqx): This should be called after internal_kv has party value, i.e.
    # after `ray.init` and
    # `internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))`
    setup_logger(
        logging_level=logging_level,
        logging_format=constants.RAYFED_LOG_FMT,
        date_format=constants.RAYFED_DATE_FMT,
        party_val=_get_party(),
    )

    logger.info(f'Started rayfed with {cluster_config}')
    set_exit_on_failure_sending(exit_on_failure_cross_silo_sending)
    # Start recv proxy
    start_recv_proxy(
        cluster=cluster,
        party=party,
        logging_level=logging_level,
        tls_config=tls_config,
        retry_policy=cross_silo_grpc_retry_policy,
    )
    start_send_proxy(
        cluster=cluster,
        party=party,
        logging_level=logging_level,
        tls_config=tls_config,
        retry_policy=cross_silo_grpc_retry_policy,
        max_retries=cross_silo_send_max_retries,
    )

    if enable_waiting_for_other_parties_ready:
        # TODO(zhouaihui): can be removed after we have a better retry strategy.
        ping_others(cluster=cluster, self_party=party, tls_config=tls_config)


def shutdown():
    """
    Shutdown a RayFed client.
    """
    wait_sending()
    compatible_utils.kv.delete(constants.KEY_OF_CLUSTER_CONFIG)
    compatible_utils.kv.delete(constants.KEY_OF_JOB_CONFIG)
    compatible_utils.kv.reset()
    ray.shutdown()
    logger.info('Shutdowned ray.')


def _get_cluster():
    """
    Get the RayFed cluster configration.
    """
    return fed_config.get_cluster_config().cluster_addresses


def _get_party():
    """
    A private util function to get the current party name.
    """
    return fed_config.get_cluster_config().current_party


def _get_tls():
    """
    Get the tls configurations on this party.
    """
    return fed_config.get_cluster_config().tls_config


class FedRemoteFunction:
    def __init__(self, func_or_class) -> None:
        self._node_party = None
        self._func_body = func_or_class
        self._options = {}
        self._fed_call_holder = None

    def party(self, party: str):
        self._node_party = party
        # assert self._fed_call_holder is None
        # TODO(qwang): This should be refined, to make sure we don't reuse the object
        # twice.
        self._fed_call_holder = FedCallHolder(
            self._node_party, self._execute_impl, self._options
        )
        return self

    def options(self, **options):
        self._options = options
        if self._fed_call_holder:
            self._fed_call_holder.options(**options)
        return self

    def remote(self, *args, **kwargs):
        assert (
            self._node_party is not None
        ), "A fed function should be specified within a party to execute."
        return self._fed_call_holder.internal_remote(*args, **kwargs)

    def _execute_impl(self, args, kwargs):
        return (
            ray.remote(self._func_body).options(**self._options).remote(*args, **kwargs)
        )


class FedRemoteClass:
    def __init__(self, func_or_class) -> None:
        self._party = None
        self._cls = func_or_class
        self._options = {}

    def party(self, party: str):
        self._party = party
        return self

    def options(self, **options):
        self._options = options
        return self

    def remote(self, *cls_args, **cls_kwargs):
        fed_class_task_id = get_global_context().next_seq_id()
        fed_actor_handle = FedActorHandle(
            fed_class_task_id,
            _get_cluster(),
            self._cls,
            _get_party(),
            self._party,
            self._options,
        )
        fed_call_holder = FedCallHolder(
            self._party, fed_actor_handle._execute_impl, self._options
        )
        fed_call_holder.internal_remote(*cls_args, **cls_kwargs)
        return fed_actor_handle


# This is the decorator `@fed.remote`
def remote(*args, **kwargs):
    def _make_fed_remote(function_or_class, **options):
        if inspect.isfunction(function_or_class) or fed_utils.is_cython(
            function_or_class
        ):
            return FedRemoteFunction(function_or_class).options(**options)

        if inspect.isclass(function_or_class):
            return FedRemoteClass(function_or_class).options(**options)

        raise TypeError(
            "The @fed.remote decorator must be applied to either a function or a class."
        )

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @fed.remote.
        return _make_fed_remote(args[0])
    assert len(args) == 0 and len(kwargs) > 0, "Remote args error."
    return functools.partial(_make_fed_remote, **kwargs)


def get(
    fed_objects: Union[ray.ObjectRef, List[FedObject], FedObject, List[FedObject]]
) -> Any:
    """
    Gets the real data of the given fed_object.

    If the object is located in current party, return it immediately,
    otherwise return it after receiving the real data from the located
    party.
    """
    if is_ray_object_refs(fed_objects):
        return ray.get(fed_objects)

    # A fake fed_task_id for a `fed.get()` operator. This is useful
    # to help contruct the whole DAG within `fed.get`.
    fake_fed_task_id = get_global_context().next_seq_id()
    cluster = _get_cluster()
    current_party = _get_party()
    is_individual_id = isinstance(fed_objects, FedObject)
    if is_individual_id:
        fed_objects = [fed_objects]

    ray_refs = []
    for fed_object in fed_objects:
        if fed_object.get_party() == current_party:
            # The code path of the fed_object is in current party, so
            # need to boardcast the data of the fed_object to other parties,
            # and then return the real data of that.
            ray_object_ref = fed_object.get_ray_object_ref()
            assert ray_object_ref is not None
            ray_refs.append(ray_object_ref)

            for party_name in cluster:
                if party_name == current_party:
                    continue
                else:
                    if fed_object._was_sending_or_sent_to_party(party_name):
                        # This object was sending or sent to the target party,
                        # so no need to do it again.
                        continue
                    else:
                        fed_object._mark_is_sending_to_party(party_name)
                        send(
                            dest_party=party_name,
                            data=ray_object_ref,
                            upstream_seq_id=fed_object.get_fed_task_id(),
                            downstream_seq_id=fake_fed_task_id,
                        )
        else:
            # This is the code path that the fed_object is not in current party.
            # So we should insert a `recv_op` as a barrier to receive the real
            # data from the location party of the fed_object.
            if fed_object.get_ray_object_ref() is not None:
                received_ray_object_ref = fed_object.get_ray_object_ref()
            else:
                received_ray_object_ref = recv(
                    current_party,
                    fed_object.get_party(),
                    fed_object.get_fed_task_id(),
                    fake_fed_task_id,
                )
                fed_object._cache_ray_object_ref(received_ray_object_ref)
            ray_refs.append(received_ray_object_ref)

    values = ray.get(ray_refs)
    if is_individual_id:
        values = values[0]

    return values


def kill(actor: FedActorHandle, *, no_restart=True):
    current_party = _get_party()
    if actor._node_party == current_party:
        handler = actor._actor_handle
        ray.kill(handler, no_restart=no_restart)
