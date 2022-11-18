import functools
import inspect
import logging
from typing import Any, Dict, List, Union

import cloudpickle
import ray
import ray.experimental.internal_kv as internal_kv
from ray._private.gcs_utils import GcsClient
from ray._private.inspect_util import is_cython

from fed._private.constants import (RAYFED_CLUSTER_KEY, RAYFED_DATE_FMT,
                                    RAYFED_LOG_FMT, RAYFED_PARTY_KEY,
                                    RAYFED_TLS_CONFIG)
from fed._private.fed_actor import FedActorHandle
from fed._private.fed_call_holder import FedCallHolder
from fed._private.global_context import get_global_context
from fed.barriers import recv, send, start_recv_proxy
from fed.fed_object import FedObject
from fed.utils import setup_logger, is_ray_object_refs

logger = logging.getLogger(__name__)

def init(address: str=None,
         cluster: Dict=None,
         party: str=None,
         tls_config:Dict=None,
         *args,
         **kwargs):
    """
    Initialize a RayFed client. it connects an exist cluster
    if address provided, otherwise start a new local cluster.
    """
    assert cluster, "Cluster should be provided."
    assert party, "Party should be provided."
    if address is not None:
        # Connect to an exist Ray cluster as driver.
        ray.init(adress=address, args=args, kwargs=kwargs)
    else:
        # Start a local Ray cluster.
        ray.init(*args, **kwargs)

    tls_config = {} if tls_config is None else tls_config
    # A Ray private accessing, should be replaced in public API.
    gcs_address = ray._private.worker._global_node.gcs_address
    gcs_client = GcsClient(address=gcs_address, nums_reconnect_retry=10)
    internal_kv._initialize_internal_kv(gcs_client)
    internal_kv._internal_kv_put(RAYFED_CLUSTER_KEY, cloudpickle.dumps(cluster))
    internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))
    internal_kv._internal_kv_put(RAYFED_TLS_CONFIG, cloudpickle.dumps(tls_config))

    # Set logger.
    # Note(NKcqx): This should be called after internal_kv has party value, i.e.
    # after `ray.init` and `internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))`
    setup_logger(logging_level="info",
                logging_format=RAYFED_LOG_FMT,
                date_format=RAYFED_DATE_FMT,
                party_val=get_party())
    # Start recv proxy
    start_recv_proxy(tls_config, cluster[party], party)

def shutdown():
    """
    Shutdown a RayFed client.
    """
    internal_kv._internal_kv_del(RAYFED_CLUSTER_KEY)
    internal_kv._internal_kv_del(RAYFED_PARTY_KEY)
    internal_kv._internal_kv_del(RAYFED_TLS_CONFIG)
    internal_kv._internal_kv_reset()
    ray.shutdown()


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
        self._fed_call_holder = FedCallHolder(self._node_party, self._execute_impl, self._options)
        return self

    def options(self, **options):
        self._options = options
        if self._fed_call_holder:
            self._fed_call_holder.options(**options)
        return self

    def remote(self, *args, **kwargs):
        assert self._node_party is not None, "A fed function should be specified within a party to execute."
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
        fed_call_holder = FedCallHolder(self._party, fed_actor_handle._execute_impl, self._options)
        fed_obj_ref = fed_call_holder.internal_remote(*cls_args, **cls_kwargs)
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


def get(fed_objects: Union[ray.ObjectRef, List[FedObject], FedObject, List[FedObject]]) -> Any:
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
    tls_config = get_tls()
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

            for party_name, party_addr in cluster.items():
                if party_name == current_party:
                    continue
                else:
                    send(
                        tls_config,
                        party_name,
                        current_party,
                        party_addr,
                        ray_object_ref,
                        fed_object.get_fed_task_id(),
                        fake_fed_task_id,
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
        ray.kill(handler, no_restart)
