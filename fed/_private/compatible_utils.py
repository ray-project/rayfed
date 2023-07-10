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

import abc
import ray
import fed._private.constants as fed_constants

import ray.experimental.internal_kv as ray_internal_kv
from fed._private import constants


def _compare_version_strings(version1, version2):
    """
    This utility function compares two version strings and returns
    True if version1 is greater, and False if they're equal, and
    False if version2 is greater.
    """
    v1_list = version1.split('.')
    v2_list = version2.split('.')
    len1 = len(v1_list)
    len2 = len(v2_list)

    for i in range(min(len1, len2)):
        if v1_list[i] == v2_list[i]:
            continue
        else:
            break

    return int(v1_list[i]) > int(v2_list[i])


def _ray_version_less_than_2_0_0():
    """ Whther the current ray version is less 2.0.0.
    """
    return _compare_version_strings(
        fed_constants.RAY_VERSION_2_0_0_STR, ray.__version__)


def init_ray(address: str = None, **kwargs):
    """A compatible API to init Ray.
    """
    if address == 'local' and _ray_version_less_than_2_0_0():
        # Ignore the `local` when ray < 2.0.0
        ray.init(**kwargs)
    else:
        ray.init(address=address, **kwargs)


def _get_gcs_address_from_ray_worker():
    """A compatible API to get the gcs address from the ray worker module.
    """
    try:
        return ray._private.worker._global_node.gcs_address
    except AttributeError:
        return ray.worker._global_node.gcs_address


class AbstractInternalKv(abc.ABC):
    """ An abstract class that represents for bridging Ray internal kv in
    both Ray client mode and non Ray client mode.
    """
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def initialize(self):
        pass

    @abc.abstractmethod
    def put(self, k, v):
        pass

    @abc.abstractmethod
    def get(self, k):
        pass

    @abc.abstractmethod
    def delete(self, k):
        pass

    @abc.abstractmethod
    def reset(self):
        pass


class InternalKv(AbstractInternalKv):
    """The internal kv class for non Ray client mode.
    """
    def __init__(self) -> None:
        super().__init__()

    def initialize(self):
        try:
            from ray._private.gcs_utils import GcsClient
        except ImportError:
            # The GcsClient was moved to `ray._raylet` module in `ray-2.5.0`.
            assert _compare_version_strings(ray.__version__, "2.4.0")
            from ray._raylet import GcsClient

        gcs_client = GcsClient(
            address=_get_gcs_address_from_ray_worker(),
            nums_reconnect_retry=10)
        return ray_internal_kv._initialize_internal_kv(gcs_client)

    def put(self, k, v):
        return ray_internal_kv._internal_kv_put(k, v)

    def get(self, k):
        return ray_internal_kv._internal_kv_get(k)

    def delete(self, k):
        return ray_internal_kv._internal_kv_del(k)

    def reset(self):
        return ray_internal_kv._internal_kv_reset()

    def _ping(self):
        return "pong"


class ClientModeInternalKv(AbstractInternalKv):
    """The internal kv class for Ray client mode.
    """
    def __init__(self) -> None:
        super().__init__()
        self._internal_kv_actor = ray.get_actor("_INTERNAL_KV_ACTOR")

    def initialize(self):
        o = self._internal_kv_actor.initialize.remote()
        return ray.get(o)

    def put(self, k, v):
        o = self._internal_kv_actor.put.remote(k, v)
        return ray.get(o)

    def get(self, k):
        o = self._internal_kv_actor.get.remote(k)
        return ray.get(o)

    def delete(self, k):
        o = self._internal_kv_actor.delete.remote(k)
        return ray.get(o)

    def reset(self):
        o = self._internal_kv_actor.reset.remote()
        return ray.get(o)


def _init_internal_kv():
    """An internal API that initialize the internal kv object."""
    global kv
    if kv is None:
        from ray._private.client_mode_hook import is_client_mode_enabled
        if is_client_mode_enabled:
            kv_actor = ray.remote(InternalKv).options(
                name="_INTERNAL_KV_ACTOR").remote()
            response = kv_actor._ping.remote()
            ray.get(response)
        kv = ClientModeInternalKv() if is_client_mode_enabled else InternalKv()
        kv.initialize()


def _clear_internal_kv():
    global kv
    if kv is not None:
        kv.delete(constants.KEY_OF_CLUSTER_CONFIG)
        kv.delete(constants.KEY_OF_JOB_CONFIG)
        kv.reset()
        from ray._private.client_mode_hook import is_client_mode_enabled
        if is_client_mode_enabled:
            _internal_kv_actor = ray.get_actor("_INTERNAL_KV_ACTOR")
            ray.kill(_internal_kv_actor)
        kv = None


kv = None
