import logging
from fed.fed_object import FedObject
from fed.barriers import recv
import jax
import ray

logger = logging.getLogger(__name__)


def resolve_dependencies(current_party, current_fed_task_id, *args, **kwargs):
    flattened_args, tree = jax.tree_util.tree_flatten((args, kwargs))
    indexes = []
    resolved = []
    for idx, arg in enumerate(flattened_args):
        if isinstance(arg, FedObject):
            indexes.append(idx)
            if arg.get_party() == current_party:
                logger.debug(
                    f"[{current_party}] Insert fed object, arg.party={arg.get_party()}"
                )
                resolved.append(arg.get_ray_object_ref())
            else:
                logger.debug(
                    f"[{current_party}] Insert recv_op, arg task id {arg.get_fed_task_id()}, current task id {current_fed_task_id}"
                )
                recv_obj = recv(
                    current_party, arg.get_fed_task_id(), current_fed_task_id
                )
                resolved.append(recv_obj)
    if resolved:
        actual_vals = ray.get(resolved)
        for idx, actual_val in zip(indexes, actual_vals):
            flattened_args[idx] = actual_val

    resolved_args, resolved_kwargs = jax.tree_util.tree_unflatten(tree, flattened_args)
    return resolved_args, resolved_kwargs
