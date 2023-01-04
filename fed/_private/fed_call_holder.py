import logging

# Set config in the very beginning to avoid being overwritten by other packages
logging.basicConfig(level=logging.INFO)

import jax

import fed
from fed._private.global_context import get_global_context
from fed.barriers import send
from fed.fed_object import FedObject
from fed.utils import resolve_dependencies

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
    def __init__(self, node_party, submit_ray_task_func, options = {}) -> None:
        self._party = fed.get_party()
        self._node_party = node_party
        self._options = options
        self._submit_ray_task_func = submit_ray_task_func
    
    def options(self, **options):
        self._options = options
        return self

    def internal_remote(self, invoking_frame=None, *args, **kwargs):
        assert invoking_frame is not None
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
                    FedObject(self._node_party, fed_task_id, ref, i, invoking_frame)
                    for i, ref in enumerate(ray_obj_ref)
                ]
            else:
                return FedObject(self._node_party, fed_task_id, ray_obj_ref, None, invoking_frame)
        else:
            flattened_args, _ = jax.tree_util.tree_flatten((args, kwargs))
            for arg in flattened_args:
                # TODO(qwang): We still need to cosider kwargs and a deeply object_ref in this party.
                if isinstance(arg, FedObject) and arg.get_party() == self._party:
                    send(
                        self._node_party,
                        arg.get_ray_object_ref(),
                        arg.get_fed_task_id(),
                        fed_task_id,
                        self._node_party,
                        tls_config=None,
                        invoking_frame=invoking_frame,
                    )
            if (
                self._options
                and 'num_returns' in self._options
                and self._options['num_returns'] > 1
            ):
                num_returns = self._options['num_returns']
                return [FedObject(self._node_party, fed_task_id, None, i, invoking_frame) for i in range(num_returns)]
            else:
                return FedObject(self._node_party, fed_task_id, None, None, invoking_frame)
