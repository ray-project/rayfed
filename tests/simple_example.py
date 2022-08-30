from threading import local
from fed.api import FedDAG
from ray.dag.input_node import InputNode
from typing import Any, TypeVar

from fed.api import build_fed_dag

import ray
import click

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


@click.command()
@click.option(
    "--party-name",
    required=True,
    type=str,
)
def main(party_name: str) -> FedDAG:
    res_dict = {
        f"RES_{party_name}": 1000
    }
    ray.init(resources=res_dict)

    print(f"=============party name is {party_name}")

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.options(resources={"RES_ALICE": 1}).bind(ds1)
    actor_bob = MyActor.options(resources={"RES_BOB": 1}).bind(ds2)

    obj_alice_f = actor_alice.f.options(resources={"RES_ALICE": 1}).bind()
    obj_bob_f = actor_bob.f.options(resources={"RES_BOB": 1}).bind()

    obj_alice_g = actor_alice.g.options(resources={"RES_ALICE": 1}).bind(obj_alice_f)
    obj_bob_h = actor_bob.h.options(resources={"RES_BOB": 1}).bind(obj_bob_f)

    obj_agg_fn = agg_fn.options(resources={"RES_BOB": 1}).bind(obj_alice_g, obj_bob_h)
    fed_dag = build_fed_dag(obj_agg_fn, party_name)
    fed_dag.execute()
    ray.shutdown()

if __name__ == "__main__":
    main()
