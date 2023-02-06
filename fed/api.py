# Copyright 2022 Ant Group Co., Ltd.
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
import ray.experimental.internal_kv as internal_kv
from ray._private.gcs_utils import GcsClient
from ray._private.inspect_util import is_cython

from fed._private.constants import (
    RAYFED_CLUSTER_KEY,
    RAYFED_DATE_FMT,
    RAYFED_LOG_FMT,
    RAYFED_PARTY_KEY,
    RAYFED_TLS_CONFIG,
    RAYFED_CROSS_SILO_SERIALIZING_ALLOWED_LIST,
)
from fed._private.fed_actor import FedActorHandle
from fed._private.fed_call_holder import FedCallHolder
from fed._private.global_context import get_global_context
from fed._private.grpc_options import set_max_message_length
from fed.barriers import recv, send, start_recv_proxy, start_send_proxy
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
    grpc_max_size_in_bytes: int = None,
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

            .. code:: python
                {
                    "cert": {
                        "ca_cert": "cacert.pem",
                        "cert": "servercert.pem",
                        "key": "serverkey.pem",
                    },
                    "client_certs": {
                        "bob":  {
                            "ca_cert": "bob's cacert.pem",
                            "cert": "bob's servercert.pem",
                        }
                    }
                }
        logging_level: optional; the logging level, could be `debug`, `info`,
            `warning`, `error`, `critical`, not case sensititive.
        cross_silo_grpc_retry_policy: a dict descibes the retry policy for
            cross silo rpc call. If None, the following default retry policy
            will be used. More details please refer to
            `retry-policy <https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy>`_.

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
        cross_silo_serializing_allowed_list: The package or class list allowed for serializing(deserializating)
            cross silos. It's used for avoiding pickle deserializing execution attack when crossing solis.
        exit_on_failure_cross_silo_sending: whether exit when failure on
            cross-silo sending. If True, a SIGTERM will be signaled to self
            if failed to sending cross-silo data.
        grpc_max_size_in_bytes: The maximum length in bytes of gRPC messages.
            If None, the default value of 500 MB is specified.
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
    ray.init(address=address, **kwargs)

    tls_config = {} if tls_config is None else tls_config
    # A Ray private accessing, should be replaced in public API.
    gcs_address = ray._private.worker._global_node.gcs_address
    gcs_client = GcsClient(address=gcs_address, nums_reconnect_retry=10)
    internal_kv._initialize_internal_kv(gcs_client)
    internal_kv._internal_kv_put(RAYFED_CLUSTER_KEY, cloudpickle.dumps(cluster))
    internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))
    internal_kv._internal_kv_put(RAYFED_TLS_CONFIG, cloudpickle.dumps(tls_config))
    internal_kv._internal_kv_put(RAYFED_CROSS_SILO_SERIALIZING_ALLOWED_LIST,
                                 cloudpickle.dumps(cross_silo_serializing_allowed_list))
    # Set logger.
    # Note(NKcqx): This should be called after internal_kv has party value, i.e.
    # after `ray.init` and `internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))`
    setup_logger(
        logging_level=logging_level,
        logging_format=RAYFED_LOG_FMT,
        date_format=RAYFED_DATE_FMT,
        party_val=get_party(),
    )
    set_exit_on_failure_sending(exit_on_failure_cross_silo_sending)
    set_max_message_length(grpc_max_size_in_bytes)
    # Start recv proxy
    start_recv_proxy(
        cluster=cluster,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=cross_silo_grpc_retry_policy,
    )
    start_send_proxy(
        cluster=cluster,
        party=party,
        tls_config=tls_config,
        logging_level=logging_level,
        retry_policy=cross_silo_grpc_retry_policy,
        max_retries=cross_silo_send_max_retries,
    )


def shutdown():
    """
    Shutdown a RayFed client.
    """
    wait_sending()
    internal_kv._internal_kv_del(RAYFED_CLUSTER_KEY)
    internal_kv._internal_kv_del(RAYFED_PARTY_KEY)
    internal_kv._internal_kv_del(RAYFED_TLS_CONFIG)
    internal_kv._internal_kv_del(RAYFED_CROSS_SILO_SERIALIZING_ALLOWED_LIST)
    internal_kv._internal_kv_reset()
    ray.shutdown()
    logger.info('Shutdowned ray.')


def get_cluster():
    """
    Get the RayFed cluster configration.
    """
    # TODO(qwang): These getter could be cached in local.
    serialized = internal_kv._internal_kv_get(RAYFED_CLUSTER_KEY)
    return cloudpickle.loads(serialized)


def get_party():
    """
    Get the current party name.
    """
    serialized = internal_kv._internal_kv_get(RAYFED_PARTY_KEY)
    return cloudpickle.loads(serialized)


def get_tls():
    """
    Get the tls configurations on this party.
    """
    serialized = internal_kv._internal_kv_get(RAYFED_TLS_CONFIG)
    return cloudpickle.loads(serialized)


class FedRemoteFunction:
    def __init__(self, func_or_class) -> None:
        self._node_party = None
        self._func_body = func_or_class
        self._options = {}
        self._fed_call_holder = None

    def party(self, party: str):
        self._node_party = party
        # assert self._fed_call_holder is None
        # TODO(qwang): This should be refined, to make sure we don't reuse the object twice.
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
            get_cluster(),
            self._cls,
            get_party(),
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
        if inspect.isfunction(function_or_class) or is_cython(function_or_class):
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
    cluster = get_cluster()
    current_party = get_party()
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
                    send(
                        party_name,
                        ray_object_ref,
                        fed_object.get_fed_task_id(),
                        fake_fed_task_id,
                        party_name,
                    )
        else:
            # This is the code path that the fed_object is not in current party.
            # So we should insert a `recv_op` as a barrier to receive the real
            # data from the location party of the fed_object.
            recv_obj = recv(
                current_party, fed_object.get_fed_task_id(), fake_fed_task_id
            )
            ray_refs.append(recv_obj)

    values = ray.get(ray_refs)
    if is_individual_id:
        values = values[0]

    return values


def kill(actor: FedActorHandle, *, no_restart=True):
    current_party = get_party()
    if actor._node_party == current_party:
        handler = actor._actor_handle
        ray.kill(handler, no_restart=no_restart)
