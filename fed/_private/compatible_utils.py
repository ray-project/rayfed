# Copyright 2022 The RayFed Team
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

import ray
import fed._private.constants as fed_constants


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


def get_gcs_address_from_ray_worker():
    """A compatible API to get the gcs address from the ray worker module.
    """
    try:
        return ray._private.worker._global_node.gcs_address
    except AttributeError:
        return ray.worker._global_node.gcs_address
