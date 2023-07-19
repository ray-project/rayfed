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

import io
import cloudpickle

import fed.config as fed_config


_pickle_whitelist = None


def _restricted_loads(
    serialized_data,
    *,
    fix_imports=True,
    encoding="ASCII",
    errors="strict",
    buffers=None,
):
    from sys import version_info
    assert version_info.major == 3

    if version_info.minor >= 8:
        import pickle as pickle
    else:
        import pickle5 as pickle

    class RestrictedUnpickler(pickle.Unpickler):
        def find_class(self, module, name):
            if _pickle_whitelist is None or (
                module in _pickle_whitelist
                and (_pickle_whitelist[module] is None or name in _pickle_whitelist[
                    module])
            ):
                return super().find_class(module, name)

            if module == "fed._private":  # TODO(qwang): Not sure if it works.
                return super().find_class(module, name)

            # Forbid everything else.
            raise pickle.UnpicklingError("global '%s.%s' is forbidden" % (module, name))

    if isinstance(serialized_data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(serialized_data)
    return RestrictedUnpickler(
        file, fix_imports=fix_imports, buffers=buffers, encoding=encoding, errors=errors
    ).load()


def _apply_loads_function_with_whitelist():
    global _pickle_whitelist

    _pickle_whitelist = fed_config.get_job_config() \
        .cross_silo_message_config.serializing_allowed_list
    if _pickle_whitelist is None:
        return

    if "*" in _pickle_whitelist:
        _pickle_whitelist = None
        return

    for module, attr_list in _pickle_whitelist.items():
        if attr_list is not None and "*" in attr_list:
            _pickle_whitelist[module] = None
    cloudpickle.loads = _restricted_loads
