import functools
import inspect
import logging
from typing import Any, Dict, List, Union

# Set config in the very beginning to avoid being overwritten by other packages
logging.basicConfig(level=logging.INFO)

import cloudpickle
import jax
import ray
from ray._private.inspect_util import is_cython

from fed._private.global_context import get_global_context
from fed.barriers import recv, send, start_recv_proxy
from fed.fed_object import FedObject
from fed.utils import resolve_dependencies
from fed._private.constants import RAYFED_CLUSTER_KEY, RAYFED_PARTY_KEY

import fed

logger = logging.getLogger(__name__)

"""
`FedCallHolder` represents a call node holder when submitting tasks.
For example,

  f.party("ALICE").remote()
  ~~~~~~~~~~~~~~~~
      ^
      |
it's a holder.

"""
class FedCallHolder:
    def __init__(self, node_party, submit_ray_task_func) -> None:
        self._party = fed.get_party()
        self._node_party = node_party
        self._options = {}
        self._submit_ray_task_func = submit_ray_task_func

    def internal_remote(self, *args, **kwargs):
       # Generate a new fed task id for this call.
        fed_task_id = get_global_context().next_seq_id()
        if self._party == self._node_party:
            resolved_args, resolved_kwargs = resolve_dependencies(
                self._party, fed_task_id, *args, **kwargs
            )
            # TODO(qwang): Handle kwargs.
            ray_obj_ref = self._submit_ray_task_func(resolved_args, resolved_kwargs)
            if isinstance(ray_obj_ref, list):
                return [
                    FedObject(self._node_party, fed_task_id, ref, i)
                    for i, ref in enumerate(ray_obj_ref)
                ]
            else:
                return FedObject(self._node_party, fed_task_id, ray_obj_ref)
        else:
            flattened_args, _ = jax.tree_util.tree_flatten((args, kwargs))
            for arg in flattened_args:
                # TODO(qwang): We still need to cosider kwargs and a deeply object_ref in this party.
                if isinstance(arg, FedObject) and arg.get_party() == self._party:
                    cluster = fed.get_cluster()
                    tls_config = fed.get_tls()
                    send(
                        tls_config,
                        self._node_party,
                        self._party,
                        cluster[self._node_party],
                        arg.get_ray_object_ref(),
                        arg.get_fed_task_id(),
                        fed_task_id,
                    )
            if (
                self._options
                and 'num_returns' in self._options
                and self._options['num_returns'] > 1
            ):
                num_returns = self._options['num_returns']
                return [FedObject(self._node_party, fed_task_id, None, i) for i in range(num_returns)]
            else:
                return FedObject(self._node_party, fed_task_id, None)
