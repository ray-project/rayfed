import logging

import jax
import ray

from fed.fed_object import FedObject

logger = logging.getLogger(__name__)


def resolve_dependencies(current_party, current_fed_task_id, *args, **kwargs):
    from fed.barriers import recv
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
                    current_party, arg.get_fed_task_id(), current_fed_task_id, arg.get_invoking_frame()
                )
                resolved.append(recv_obj)
    if resolved:
        for idx, actual_val in zip(indexes, resolved):
            flattened_args[idx] = actual_val

    resolved_args, resolved_kwargs = jax.tree_util.tree_unflatten(tree, flattened_args)
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


def setup_logger(logging_level, logging_format, date_format, log_dir=None, party_val=None):
    class PartyRecordFilter(logging.Filter):
        def __init__(self, party_val = None) -> None:
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


def _load_from_cert_config(cert_config):
    private_key_file = cert_config["key"]
    cert_file = cert_config["cert"]
    ca_cert_file = cert_config["ca_cert"]

    with open(ca_cert_file, "rb") as file:
        ca_cert = file.read()
    with open(private_key_file, "rb") as file:
        private_key = file.read()
    with open(cert_file, "rb") as file:
        cert_chain = file.read()

    return ca_cert, private_key, cert_chain

def load_server_certs(tls_config):
    assert tls_enabled(tls_config)
    server_cert_config = tls_config["cert"]
    return _load_from_cert_config(server_cert_config)


def load_client_certs(tls_config, target_party: str=None):
    assert tls_enabled(tls_config)
    all_clients = tls_config["client_certs"]
    client_cert_config = all_clients[target_party]
    return _load_from_cert_config(client_cert_config)

def error_if_dag_nodes_are_unaligned(source_invoking_frame, curr_invoking_frame):
            #  assert invoking_frame.filename == source_invoking_frame.filename
            # assert invoking_frame.lineno == source_invoking_frame.lineno, f"source_line_no={source_invoking_frame.lineno}, but curr_line_no={invoking_frame.lineno}"
            # assert invoking_frame.name == source_invoking_frame.name
    if source_invoking_frame.filename != curr_invoking_frame.filename:
        # TODO(qwang): This is too restrict to use, because the full pathes are usually not the same in different nodes.
        raise ValueError(f"source filename is {source_invoking_frame.filename}, but current filename is {curr_invoking_frame.filename}")
    elif source_invoking_frame.lineno != curr_invoking_frame.lineno:
        raise ValueError(f"source lineno is {source_invoking_frame.lineno}, but current lineno is {curr_invoking_frame.lineno}")
    elif source_invoking_frame.name != curr_invoking_frame.name:
        raise ValueError(f"source function name is {source_invoking_frame.name}, but current function name is {curr_invoking_frame.name}")
