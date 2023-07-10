

"""This module should be cached locally due to all configurations
   are mutable.
"""

import fed._private.compatible_utils as compatible_utils
import fed._private.constants as fed_constants
import cloudpickle
from typing import Dict, List, Optional
import json


class ClusterConfig:
    """A local cache of cluster configuration items."""
    def __init__(self, raw_bytes: bytes) -> None:
        self._data = cloudpickle.loads(raw_bytes)

    @property
    def cluster_addresses(self):
        return self._data[fed_constants.KEY_OF_CLUSTER_ADDRESSES]

    @property
    def current_party(self):
        return self._data[fed_constants.KEY_OF_CURRENT_PARTY_NAME]

    @property
    def tls_config(self):
        return self._data[fed_constants.KEY_OF_TLS_CONFIG]

    @property
    def serializing_allowed_list(self):
        return self._data[fed_constants.KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST]

    @property
    def cross_silo_timeout(self):
        return self._data[fed_constants.KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS]

    @property
    def cross_silo_messages_max_size(self):
        return self._data[fed_constants.KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES]


class JobConfig:
    def __init__(self, raw_bytes: bytes) -> None:
        if raw_bytes is None:
            self._data = {}
        else:
            self._data = cloudpickle.loads(raw_bytes)

    @property
    def cross_silo_comm_config(self):
        return self._data.get(fed_constants.KEY_OF_CROSS_SILO_COMM_CONFIG, {})


# A module level cache for the cluster configurations.
_cluster_config = None

_job_config = None


def get_cluster_config():
    """This function is not thread safe to use."""
    global _cluster_config
    if _cluster_config is None:
        compatible_utils._init_internal_kv()
        compatible_utils.kv.initialize()
        raw_dict = compatible_utils.kv.get(fed_constants.KEY_OF_CLUSTER_CONFIG)
        _cluster_config = ClusterConfig(raw_dict)
    return _cluster_config


def get_job_config():
    """This config still acts like cluster config for now"""
    global _job_config
    if _job_config is None:
        compatible_utils._init_internal_kv()
        compatible_utils.kv.initialize()
        raw_dict = compatible_utils.kv.get(fed_constants.KEY_OF_JOB_CONFIG)
        _job_config = JobConfig(raw_dict)
    return _job_config


class CrossSiloCommConfig:
    """A class to store parameters used for Proxy Actor

    Attributes:
        proxier_fo_max_retries: The max restart times for the send proxy.
        serializing_allowed_list: The package or class list allowed for
            serializing(deserializating) cross silos. It's used for avoiding pickle
            deserializing execution attack when crossing solis.
        send_resource_label: Customized resource label, the SendProxyActor
            will be scheduled based on the declared resource label. For example,
            when setting to `{"my_label": 1}`, then the SendProxyActor will be started
            only on Nodes with `{"resource": {"my_label": $NUM}}` where $NUM >= 1.
        recv_resource_label: Customized resource label, the RecverProxyActor
            will be scheduled based on the declared resource label. For example,
            when setting to `{"my_label": 1}`, then the RecverProxyActor will be started
            only on Nodes with `{"resource": {"my_label": $NUM}}` where $NUM >= 1.
        exit_on_sending_failure: whether exit when failure on
            cross-silo sending. If True, a SIGTERM will be signaled to self
            if failed to sending cross-silo data.
        messages_max_size_in_bytes: The maximum length in bytes of
            cross-silo messages.
            If None, the default value of 500 MB is specified.
        timeout_in_seconds: The timeout in seconds of a cross-silo RPC call.
            It's 60 by default.
        http_header: The HTTP header, e.g. metadata in grpc, sent with the RPC request.
            This won't override basic tcp headers, such as `user-agent`, but concat
            them together.
    """
    def __init__(
            self,
            proxier_fo_max_retries: int = None,
            timeout_in_seconds: int = 60,
            messages_max_size_in_bytes: int = None,
            exit_on_sending_failure: Optional[bool] = False,
            serializing_allowed_list: Optional[Dict[str, str]] = None,
            send_resource_label: Optional[Dict[str, str]] = None,
            recv_resource_label: Optional[Dict[str, str]] = None,
            http_header: Optional[Dict[str, str]] = None) -> None:
        self.proxier_fo_max_retries = proxier_fo_max_retries
        self.timeout_in_seconds = timeout_in_seconds
        self.messages_max_size_in_bytes = messages_max_size_in_bytes
        self.exit_on_sending_failure = exit_on_sending_failure
        self.serializing_allowed_list = serializing_allowed_list
        self.send_resource_label = send_resource_label
        self.recv_resource_label = recv_resource_label
        self.http_header = http_header

    def __json__(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)


class CrossSiloGrpcCommConfig(CrossSiloCommConfig):
    """A class to store parameters used for GRPC communication

    Attributes:
        grpc_retry_policy: a dict descibes the retry policy for
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
        grpc_channel_options: A list of tuples to store GRPC channel options,
            e.g. [
                    ('grpc.enable_retries', 1),
                    ('grpc.max_send_message_length', 50 * 1024 * 1024)
                ]
    """
    def __init__(self,
                 grpc_channel_options: List = None,
                 grpc_retry_policy: Dict[str, str] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.grpc_retry_policy = grpc_retry_policy
        self.grpc_channel_options = grpc_channel_options


class CrossSiloBRPCConfig(CrossSiloCommConfig):
    """A class to store parameters used for GRPC communication

    Attributes:
    """
    def __init__(self,
                 brpc_options,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.brpc_options = brpc_options
