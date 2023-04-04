

"""This module should be cached locally due to all configurations
   are mutable.
"""

import fed._private.compatible_utils as compatible_utils
import fed._private.constants as fed_constants
import cloudpickle


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
    def grpc_metadata(self):
        return self._data.get(fed_constants.KEY_OF_GRPC_METADATA, {})


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
