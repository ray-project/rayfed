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
import re
import sys

import fed
import ray

from fed._private.compatible_utils import _compare_version_strings
from fed.fed_object import FedObject

logger = logging.getLogger(__name__)


def get_package_version(package_name: str) -> str:
    """
    This utility function can retrieve the version number
     of a Python library in string format, such as '4.23.4'.
    You don't need to worry about the Python version.
    When using version 3.7 and below, it uses the built-in `pkg_resources`.
    When using Python 3.8 and above, it uses `importlib.metadata`.
    """
    curr_python_version = sys.version.split(" ")[0]
    if _compare_version_strings(curr_python_version, '3.7.99'):
        import importlib.metadata
        return importlib.metadata.version(package_name)
    else:
        import pkg_resources
        return pkg_resources.get_distribution(package_name).version


def resolve_dependencies(current_party, current_fed_task_id, *args, **kwargs):
    from fed.proxy.barriers import recv

    flattened_args, tree = fed.tree_util.tree_flatten((args, kwargs))
    indexes = []
    resolved = []
    for idx, arg in enumerate(flattened_args):
        if isinstance(arg, FedObject):
            indexes.append(idx)
            if arg.get_party() == current_party:
                logger.debug(f"Insert fed object, arg.party={arg.get_party()}")
                resolved.append(arg.get_ray_object_ref())
            else:
                logger.debug(
                    f'Insert recv_op, arg task id {arg.get_fed_task_id()}, current '
                    f'task id {current_fed_task_id}'
                )
                if arg.get_ray_object_ref() is not None:
                    # This code path indicates the ray object is already received in
                    # this party, so there is no need to receive it any longer.
                    received_ray_obj = arg.get_ray_object_ref()
                else:
                    received_ray_obj = recv(
                        current_party,
                        arg.get_party(),
                        arg.get_fed_task_id(),
                        current_fed_task_id,
                    )
                    arg._cache_ray_object_ref(received_ray_obj)
                resolved.append(received_ray_obj)
    if resolved:
        for idx, actual_val in zip(indexes, resolved):
            flattened_args[idx] = actual_val

    resolved_args, resolved_kwargs = fed.tree_util.tree_unflatten(flattened_args, tree)
    return resolved_args, resolved_kwargs


def is_ray_object_refs(objects) -> bool:
    if isinstance(objects, ray.ObjectRef):
        return True

    if isinstance(objects, list):
        for object_ref in objects:
            if not isinstance(object_ref, ray.ObjectRef):
                return False
        return True

    return False


def setup_logger(
    logging_level,
    logging_format,
    date_format,
    log_dir=None,
    party_val=None,
):
    class PartyRecordFilter(logging.Filter):
        def __init__(self, party_val=None) -> None:
            self._party_val = party_val
            super().__init__("PartyRecordFilter")

        def filter(self, record) -> bool:
            if not hasattr(record, "party"):
                record.party = self._party_val
            return True

    logger = logging.getLogger()

    # Remove default handlers otherwise a msg will be printed twice.
    for hdlr in logger.handlers:
        logger.removeHandler(hdlr)

    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)

    _formatter = logging.Formatter(fmt=logging_format, datefmt=date_format)
    _filter = PartyRecordFilter(party_val=party_val)

    _customed_handler = logging.StreamHandler()
    _customed_handler.setFormatter(_formatter)
    _customed_handler.addFilter(_filter)

    logger.addHandler(_customed_handler)


def tls_enabled(tls_config):
    return True if tls_config else False


def load_cert_config(cert_config):
    ca_cert, private_key, cert_chain = None, None, None
    if "ca_cert" in cert_config:
        with open(cert_config["ca_cert"], "rb") as file:
            ca_cert = file.read()
    with open(cert_config["key"], "rb") as file:
        private_key = file.read()
    with open(cert_config["cert"], "rb") as file:
        cert_chain = file.read()

    return ca_cert, private_key, cert_chain


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == "cython_function_or_method"

    # Check if function or method, respectively
    return check_cython(obj) or (
        hasattr(obj, "__func__") and check_cython(obj.__func__)
    )


def dict2tuple(dic):
    """
    Convert a dictionary to a two-dimensional tuple, for example:
    {'key': 'value'} => (('key', 'value'), ).
    """
    if (dic is None or isinstance(dic, tuple)):
        return dic
    elif (isinstance(dic, dict)):
        return tuple((k, v) for k, v in dic.items())
    else:
        logger.warn(f"Unable to convert type {type(dic)} to tuple"
                    f"skip converting {dic}.")
        return dic


def validate_address(address: str) -> None:
    if address is None:
        raise ValueError("The address shouldn't be None.")

    # The specific case for `local` or `localhost`.
    if address == 'local' or address == 'localhost':
        return

    # Rule 1: "ip:port" format
    ip_port_pattern = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$'
    if re.match(ip_port_pattern, address):
        return

    # Rule 2: "hostname:port" format
    hostname_port_pattern = r'^[a-zA-Z0-9.-]+:\d+$'
    if re.match(hostname_port_pattern, address):
        return

    # Rule 3: https or http link
    link_pattern = r'^(https?://).*'
    if re.match(link_pattern, address):
        return

    error_msg = ("The address format is invalid. It should be in one of the following formats:\n" # noqa
                "- `local` for starting a new cluster, or `localhost` for connecting a local cluster.\n" # noqa
                "- 'ip:port' format, where the IP needs to follow the IP address specifications and the port is a number.\n" # noqa
                "- 'hostname:port' format, where the hostname is a string and the port is a number.\n" # noqa
                "- An HTTPS or HTTP link starting with 'https://' or 'http://'.") # noqa
    raise ValueError(error_msg)


def validate_addresses(addresses: dict):
    """
    Validate whether the addresses is in correct forms.
    """
    for _, address in addresses.items():
        validate_address(address)
