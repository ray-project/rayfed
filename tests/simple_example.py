import multiprocessing

import fed
import ray
from fed.api import FedDAG, set_cluster, set_party
from fed.barriers import start_recv_proxy


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


cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}


def run(party):
    set_cluster(cluster=cluster)
    set_party(party)
    start_recv_proxy(cluster[party], party)

    print(f"Running the script in party {party}")

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.party("alice").bind(ds1)
    actor_bob = MyActor.party("bob").bind(ds2)

    obj_alice_f = actor_alice.f.bind()
    obj_bob_f = actor_bob.f.bind()

    obj_alice_g = actor_alice.g.bind(obj_alice_f)
    obj_bob_h = actor_bob.h.bind(obj_bob_f)

    obj_agg_fn = agg_fn.party("bob").bind(obj_alice_g, obj_bob_h)
    obj = obj_agg_fn.exec()
    result = fed.get(obj)
    print(f"The result in party {party} is :{result}")

    ray.shutdown()


def main() -> FedDAG:
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()


if __name__ == "__main__":
    main()
