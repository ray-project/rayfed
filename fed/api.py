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
import signal
import sys
from typing import Any, Callable, Dict, List, Union

import cloudpickle
import ray

import fed._private.compatible_utils as compatible_utils
import fed.config as fed_config
import fed.utils as fed_utils
from fed._private import constants
from fed._private.fed_actor import FedActorHandle
from fed._private.fed_call_holder import FedCallHolder
from fed._private.global_context import (
    clear_global_context,
    get_global_context,
    init_global_context,
)
from fed.config import CrossSiloMessageConfig
from fed.exceptions import FedRemoteError
from fed.fed_object import FedObject
from fed.proxy.barriers import (
    _start_receiver_proxy,
    _start_sender_proxy,
    _start_sender_receiver_proxy,
    ping_others,
    recv,
    send,
    set_proxy_actor_name,
)
from fed.proxy.base_proxy import ReceiverProxy, SenderProxy, SenderReceiverProxy
from fed.utils import is_ray_object_refs, setup_logger

logger = logging.getLogger(__name__)

original_sigint = signal.getsignal(signal.SIGINT)


def _signal_handler(signum, frame):
    if signum == signal.SIGINT:
        signal.signal(signal.SIGINT, original_sigint)
        logger.warning(
            "Stop signal received (e.g. via SIGINT/Ctrl+C), "
            "try to shutdown fed. Press CTRL+C "
            "(or send SIGINT/SIGKILL/SIGTERM) to skip."
        )
        _shutdown(intended=False)


def init(
    addresses: Dict = None,
    party: str = None,
    config: Dict = {},
    tls_config: Dict = None,
    logging_level: str = "info",
    sender_proxy_cls: SenderProxy = None,
    receiver_proxy_cls: ReceiverProxy = None,
    receiver_sender_proxy_cls: SenderReceiverProxy = None,
    job_name: str = None,
    sending_failure_handler: Callable[[Exception], None] = None,
):
    """
    Initialize a RayFed client.

    Args:
        addresses:
            optional; a dict describes the addresses configurations. E.g.

            .. code:: python

                {
                    # The address that can be connected to `alice` by other parties.
                    'alice': '127.0.0.1:10001',
                    # The address that can be connected to `bob` by other parties.
                    'bob': '127.0.0.1:10002',
                    # The address that can be connected to `carol` by other parties.
                    'carol': '127.0.0.1:10003',
                }
        party:
            optional; self party.
        config:
            optional; a dict describes general job configurations. Currently the
            supported configurations are ['cross_silo_comm', 'barrier_on_initializing'].
                cross_silo_comm
                    optional; a dict describes the cross-silo common
                    configs, the supported configs can be referred to
                    :py:meth:`fed.config.CrossSiloMessageConfig` and
                    :py:meth:`fed.config.GrpcCrossSiloMessageConfig`. Note that, the
                    `cross_silo_comm.messages_max_size_in_bytes` will be overrided
                    if `cross_silo_comm.grpc_channel_options` is provided and contains
                    `grpc.max_send_message_length` or `grpc.max_receive_message_length`.
                barrier_on_initializing
                    optional; a bool value indicates whether to
                    wait for all parties to be ready before starting the job. If set
                    to True, the job will be started after all parties are ready,
                    otherwise, the job will be started immediately after the current
                    party is ready.

            E.g.

            .. code:: python

                {
                    "cross_silo_comm": {
                        "messages_max_size_in_bytes": 500*1024,
                        "timeout_in_ms": 1000,
                        "exit_on_sending_failure": True,
                        "expose_error_trace": True,
                        "use_global_proxy": True,
                        "continue_waiting_for_data_sending_on_error": False,
                    },
                    "barrier_on_initializing": True,
                }
        tls_config:
            optional; a dict describes the tls config. E.g.
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
        logging_level:
            optional; the logging level, could be `debug`, `info`, `warning`, `error`,
            `critical`, not case sensititive.
        job_name:
            optional; the job name of the current job. Note that, the job name
            must be identical in all parties, otherwise, messages will be ignored
            because of the job name mismatch. If the job name is not provided, an
            default fixed name will be assigned, therefore messages of all anonymous
            jobs will be mixed together, which should only be used in the single job
            scenario or test mode.
        sending_failure_handler:
            optional; a callback which will be triggeed if cross-silo message sending
            failed and exit_on_sending_failure in config is True.
    Examples:
        >>> import fed
        >>> import ray
        >>> ray.init(address='local')
        >>> addresses = {
        >>>    'alice': '127.0.0.1:10001',
        >>>    'bob': '127.0.0.1:10002',
        >>>    'carol': '127.0.0.1:10003',
        >>> }
        >>> # Start as alice.
        >>> fed.init(addresses=addresses, party='alice')
    """
    assert addresses, "Addresses should be provided."
    assert party, "Party should be provided."
    assert party in addresses, f"Party {party} is not in the addresses {addresses}."

    if job_name is None:
        job_name = constants.RAYFED_DEFAULT_JOB_NAME

    fed_utils.validate_addresses(addresses)

    cross_silo_comm_dict = config.get("cross_silo_comm", {})
    cross_silo_comm_config = CrossSiloMessageConfig.from_dict(cross_silo_comm_dict)

    init_global_context(
        current_party=party,
        job_name=job_name,
        exit_on_sending_failure=cross_silo_comm_config.exit_on_sending_failure,
        continue_waiting_for_data_sending_on_error=cross_silo_comm_config.continue_waiting_for_data_sending_on_error,
        sending_failure_handler=sending_failure_handler,
    )

    tls_config = {} if tls_config is None else tls_config
    if tls_config:
        assert (
            "cert" in tls_config and "key" in tls_config
        ), "Cert or key are not in tls_config."

    # A Ray private accessing, should be replaced in public API.
    compatible_utils._init_internal_kv(job_name)

    cluster_config = {
        constants.KEY_OF_CLUSTER_ADDRESSES: addresses,
        constants.KEY_OF_CURRENT_PARTY_NAME: party,
        constants.KEY_OF_TLS_CONFIG: tls_config,
    }
    compatible_utils.kv.put(
        constants.KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config)
    )

    job_config = {
        constants.KEY_OF_CROSS_SILO_COMM_CONFIG_DICT: cross_silo_comm_dict,
    }
    compatible_utils.kv.put(constants.KEY_OF_JOB_CONFIG, cloudpickle.dumps(job_config))

    # Set logger.
    # Note(NKcqx): This should be called after internal_kv has party value, i.e.
    # after `ray.init` and
    # `internal_kv._internal_kv_put(RAYFED_PARTY_KEY, cloudpickle.dumps(party))`
    setup_logger(
        logging_level=logging_level,
        logging_format=constants.RAYFED_LOG_FMT,
        date_format=constants.RAYFED_DATE_FMT,
        party_val=_get_party(job_name),
        job_name=job_name,
    )

    logger.info(f"Started rayfed with {cluster_config}")
    signal.signal(signal.SIGINT, _signal_handler)
    get_global_context().get_cleanup_manager().start(
        exit_on_sending_failure=cross_silo_comm_config.exit_on_sending_failure,
        expose_error_trace=cross_silo_comm_config.expose_error_trace,
    )

    if receiver_sender_proxy_cls is not None:
        set_proxy_actor_name(
            job_name, cross_silo_comm_dict.get("use_global_proxy", True), True
        )
        _start_sender_receiver_proxy(
            addresses=addresses,
            party=party,
            logging_level=logging_level,
            tls_config=tls_config,
            proxy_cls=receiver_sender_proxy_cls,
            proxy_config=cross_silo_comm_dict,
            ready_timeout_second=cross_silo_comm_config.timeout_in_ms / 1000,
        )
    else:
        if receiver_proxy_cls is None:
            logger.debug(
                (
                    "No receiver proxy class specified, "
                    "use `GrpcRecvProxy` by default."
                )
            )
            from fed.proxy.grpc.grpc_proxy import GrpcReceiverProxy

            receiver_proxy_cls = GrpcReceiverProxy
        set_proxy_actor_name(
            job_name, cross_silo_comm_dict.get("use_global_proxy", True)
        )
        _start_receiver_proxy(
            addresses=addresses,
            party=party,
            logging_level=logging_level,
            tls_config=tls_config,
            proxy_cls=receiver_proxy_cls,
            proxy_config=cross_silo_comm_dict,
            ready_timeout_second=cross_silo_comm_config.timeout_in_ms / 1000,
        )

        if sender_proxy_cls is None:
            logger.debug(
                "No sender proxy class specified, use `GrpcSenderProxy` by default."
            )
            from fed.proxy.grpc.grpc_proxy import GrpcSenderProxy

            sender_proxy_cls = GrpcSenderProxy

        _start_sender_proxy(
            addresses=addresses,
            party=party,
            logging_level=logging_level,
            tls_config=tls_config,
            proxy_cls=sender_proxy_cls,
            proxy_config=cross_silo_comm_dict,
            ready_timeout_second=cross_silo_comm_config.timeout_in_ms / 1000,
        )

    if config.get("barrier_on_initializing", False):
        # TODO(zhouaihui): can be removed after we have a better retry strategy.
        ping_others(addresses=addresses, self_party=party, max_retries=3600)


def shutdown():
    """
    Shutdown a RayFed client.
    """
    global_context = get_global_context()
    if global_context is not None and global_context.acquire_shutdown_flag():
        _shutdown(True)


def _shutdown(intended=True):
    """
    Shutdown a RayFed client.

    Args:
        intended: (Optional) Whether this is a intended shutdown. If not
           a "failure handler" will be triggered and do not wait data sending.
    """

    if get_global_context() is None:
        # Do nothing since job has not been inited or is cleaned already.
        return

    if intended:
        logger.info("Shutdowning rayfed intendedly...")
    else:
        logger.warn("Shutdowning rayfed unintendedly...")
    global_context = get_global_context()
    last_sending_error = global_context.get_cleanup_manager().get_last_sending_error()
    last_received_error = global_context.get_last_recevied_error()
    if last_sending_error is not None:
        logging.error(f"Cross-silo sending error occured. {last_sending_error}")

    wait_for_sending = True
    if (
        last_sending_error is not None or last_received_error is not None
    ) and not global_context.get_continue_waiting_for_data_sending_on_error():
        wait_for_sending = False
    logging.info(f'{"Wait" if wait_for_sending else "No wait"} for data sending.')

    if not intended:
        # Execute failure_handler fisrtly.
        failure_handler = global_context.get_sending_failure_handler()
        if failure_handler is not None:
            logger.info(f"Executing failure handler {failure_handler} ...")
            failure_handler(last_sending_error)

        exit_on_sending_failure = global_context.get_exit_on_sending_failure()

        # Clean context.
        compatible_utils._clear_internal_kv()
        clear_global_context(wait_for_sending=wait_for_sending)
        logger.info("Shutdowned rayfed.")

        if exit_on_sending_failure:
            # Exit with error.
            logger.critical("Exit now due to the previous error.")
            sys.exit(1)
    else:
        # Clean context.
        compatible_utils._clear_internal_kv()
        clear_global_context(wait_for_sending=wait_for_sending)
        logger.info("Shutdowned rayfed.")


def _get_addresses(job_name: str = None):
    """
    Get the RayFed addresses configration.
    """
    return fed_config.get_cluster_config(job_name).cluster_addresses


def _get_party(job_name: str = None):
    """
    A private util function to get the current party name.
    """
    return fed_config.get_cluster_config(job_name).current_party


def _get_tls(job_name: str = None):
    """
    Get the tls configurations on this party.
    """
    return fed_config.get_cluster_config(job_name).tls_config


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
        if not self._node_party:
            raise ValueError("You should specify a party name on the fed function.")

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
        job_name = get_global_context().get_job_name()
        fed_actor_handle = FedActorHandle(
            fed_class_task_id,
            _get_addresses(job_name),
            self._cls,
            _get_party(job_name),
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
    """Defines a remote function or an actor class.

    This function can be used as a decorator with no arguments
    to define a remote function or actor as follows:

    .. testcode::

        import fed

        @fed.remote
        def f(a, b, c):
            return a + b + c

        object_ref = f.part('alice').remote(1, 2, 3)
        result = fed.get(object_ref)
        assert result == (1 + 2 + 3)

        @fed.remote
        class Foo:
            def __init__(self, arg):
                self.x = arg

            def method(self, a):
                return self.x + a

        actor_handle = Foo.party('alice').remote(123)
        object_ref = actor_handle.method.remote(321)
        result = fed.get(object_ref)
        assert result == (123 + 321)

    Equivalently, use a function call to create a remote function or actor.

    .. testcode::

        def g(a, b, c):
            return a + b + c

        remote_g = fed.remote(g)
        object_ref = remote_g.party('alice').remote(1, 2, 3)
        assert fed.get(object_ref) == (1 + 2 + 3)

        class Bar:
            def __init__(self, arg):
                self.x = arg

            def method(self, a):
                return self.x + a

        RemoteBar = fed.remote(Bar)
        actor_handle = RemoteBar.party('alice').remote(123)
        object_ref = actor_handle.method.remote(321)
        result = fed.get(object_ref)
        assert result == (123 + 321)


    It can also be used with specific keyword arguments just same as ray options.
    """

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
    job_name = get_global_context().get_job_name()
    addresses = _get_addresses(job_name)
    current_party = _get_party(job_name)
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

            for party_name in addresses:
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

    try:
        values = ray.get(ray_refs)
        if is_individual_id:
            values = values[0]
        return values
    except FedRemoteError as e:
        logger.warning(
            "Encounter RemoteError happend in other parties"
            f", error message: {e.cause}"
        )
        if get_global_context() is not None:
            get_global_context().set_last_recevied_error(e)
        raise e


def kill(actor: FedActorHandle, *, no_restart=True):
    """Kill an actor forcefully.

    Args:
        actor: Handle to the actor to kill.
        no_restart: Whether or not this actor should be restarted if
            it's a restartable actor.
    """
    job_name = get_global_context().get_job_name()
    current_party = _get_party(job_name)
    if actor._node_party == current_party:
        handler = actor._actor_handle
        ray.kill(handler, no_restart=no_restart)
