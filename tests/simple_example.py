from threading import local
from fed.api import FedDAG
from ray.dag.input_node import InputNode
from typing import Any, TypeVar

from fed.api import build_fed_dag
import fed

import ray
import click

@fed.remote
class MyActor:
    def __init__(self, data):
        self.__data = data

    def f(self):
        return f"f({ray.util.get_node_ip_address()})"

    def g(self, obj):
        return obj + "g"

    def h(self, obj):
        return obj + "h"

@fed.remote
def agg_fn(obj1, obj2):
    return f"agg-{obj1}-{obj2}"


@click.command()
@click.option(
    "--party-name",
    required=True,
    type=str,
)
def main(party_name: str) -> FedDAG:
    ray.init()
    print(f"=============party name is {party_name}")

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.party("ALICE").bind(ds1)
    actor_bob = MyActor.party("BOB").bind(ds2)

    obj_alice_f = actor_alice.f.bind()
    obj_bob_f = actor_bob.f.bind()

    obj_alice_g = actor_alice.g.bind(obj_alice_f)
    obj_bob_h = actor_bob.h.bind(obj_bob_f)

    obj_agg_fn = agg_fn.party("BOB").bind(obj_alice_g, obj_bob_h)
    print(f"==========type is {type(obj_agg_fn)}")
    fed_dag = build_fed_dag(obj_agg_fn, party_name)
    fed_dag.execute()
    ray.shutdown()


if __name__ == "__main__":
    main()
