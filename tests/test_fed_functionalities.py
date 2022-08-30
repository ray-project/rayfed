from threading import local
import pytest
from fed.api import FedDAG
from ray.dag.input_node import InputNode
from typing import Any, TypeVar

from fed.api import build_fed_dag

import ray


"""
 f1    f2           f
 |     |            |
 g1    g2           h
 \      \          /
   \     \        /
     ---\ \    /
          agg_fn
"""
def _get_seq_ids_from_uuids(fed_dag: FedDAG, uuids: list):
    """A helper that gets seq ids from uuids."""
    seq_ids = []
    all_node_info = fed_dag.get_all_node_info()
    for uuid in uuids:
        seq_id, _ = all_node_info[uuid]
        seq_ids.append(seq_id)
    return seq_ids

def _get_seq_ids_from_uuids_in_dict(fed_dag: FedDAG, uuids: list):
    """A helper that gets seq ids from uuids."""
    seq_ids = {}
    all_node_info = fed_dag.get_all_node_info()
    for key_uuid in uuids:
        key_seq_id, _ = all_node_info[key_uuid]
        value_seq_id, _ = all_node_info[uuids[key_uuid]]
        seq_ids[key_seq_id] = value_seq_id
    return seq_ids

def build_example_dag(party_name: str) -> FedDAG:
    """A helper that build an example FED DAG for the following tests."""
    @ray.remote
    class MyActor:
        def __init__(self, data):
            self.__data = data

        def f(self):
            return f"f({ray.util.get_node_ip_address()})"

        def g(self, obj):
            return obj + "g"

        def h(self, obj):
            return obj + "h"

    @ray.remote
    def agg_fn(obj1, obj2):
        return f"agg-{obj1}-{obj2}"

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.options(resources={"RES_ALICE": 1}).bind(ds1)
    actor_bob = MyActor.options(resources={"RES_BOB": 1}).bind(ds2)

    # TODO(qwang): We should remove these resources specifications.
    obj_alice_f = actor_alice.f.options(resources={"RES_ALICE": 1}).bind()
    obj_bob_f = actor_bob.f.options(resources={"RES_BOB": 1}).bind()

    obj_alice_g = actor_alice.g.options(resources={"RES_ALICE": 1}).bind(obj_alice_f)
    obj_bob_h = actor_bob.h.options(resources={"RES_BOB": 1}).bind(obj_bob_f)

    obj_agg_fn = agg_fn.options(resources={"RES_BOB": 1}).bind(obj_alice_g, obj_bob_h)
    fed_dag = build_fed_dag(obj_agg_fn, party_name)
    return fed_dag

def test_build_dag():
    ray.init(resources={
        "RES_ALICE": 100,
        "RES_BOB": 100
    })

    fed_dag = build_example_dag(party_name="ALICE")
    assert fed_dag.get_curr_seq_count() == 7

    all_node_info = fed_dag.get_all_node_info()
    assert len(all_node_info) == 7
    ray.shutdown()

def test_extract_local_dag():
    ray.init(resources={
        "RES_ALICE": 100,
        "RES_BOB": 100
    })

    # test alice party
    fed_dag = build_example_dag(party_name="ALICE")
    local_dag_uuids: list = fed_dag.get_local_dag_uuids()
    alice_seq_ids = _get_seq_ids_from_uuids(fed_dag, local_dag_uuids)
    assert len(alice_seq_ids) == 3
    assert 0 in alice_seq_ids
    assert 1 in alice_seq_ids 
    assert 2 in alice_seq_ids

    # test bob party
    fed_dag = build_example_dag(party_name="BOB")
    local_dag_uuids: list = fed_dag.get_local_dag_uuids()
    alice_seq_ids = _get_seq_ids_from_uuids(fed_dag, local_dag_uuids)
    assert len(alice_seq_ids) == 4
    assert 3 in alice_seq_ids
    assert 4 in alice_seq_ids 
    assert 5 in alice_seq_ids
    assert 6 in alice_seq_ids
    ray.shutdown()

def test_build_indexes():
    ray.init(resources={
        "RES_ALICE": 100,
        "RES_BOB": 100
    })
    fed_dag = build_example_dag(party_name="ALICE")
    upstream_index_uuids = fed_dag.get_upstream_indexes()
    downstream_index_uuids = fed_dag.get_downstream_indexes()

    up_seq_ids = _get_seq_ids_from_uuids_in_dict(fed_dag, upstream_index_uuids)
    down_seq_ids = _get_seq_ids_from_uuids_in_dict(fed_dag, downstream_index_uuids)
    assert {6: 2} == up_seq_ids
    assert {2 : 6} == down_seq_ids
    ray.shutdown()

def test_inejct_barriers_bob():
    ray.init(resources={
        "RES_ALICE": 100,
        "RES_BOB": 100
    })
    # The local dag in bob should be:
    # Bob.init  ->  Bob.f  ->  Bob.h  ->  agg_fn
    #                                       /
    #  Recver.init  ->  Recver.get_data----/
    fed_dag = build_example_dag(party_name="BOB")
    nodes_to_drive = fed_dag.get_nodes_to_drive()
    assert len(nodes_to_drive) == 1
    final_node = nodes_to_drive[0]
    agg_fn_args = final_node.get_args()
    assert len(agg_fn_args) == 2
    assert agg_fn_args[0].get_func_name() in ["recv_op", "h"]
    assert agg_fn_args[1].get_func_name() in ["recv_op", "h"]

    ray.shutdown()

def test_inejct_barriers_alice():
    ray.init(resources={
        "RES_ALICE": 100,
        "RES_BOB": 100
    })
    fed_dag = build_example_dag(party_name="ALICE")

    nodes_to_drive = fed_dag.get_nodes_to_drive()
    assert len(nodes_to_drive) == 1
    send_op_node = nodes_to_drive[0]
    send_op_node.get_func_name() == "send_op"
    args_0 = send_op_node.get_args()
    assert len(args_0) == 3
    assert args_0[0]._method_name == "g"
    ray.shutdown()

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
