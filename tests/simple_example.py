import multiprocessing
from sqlite3 import paramstyle

import fed
import ray
from fed.api import set_cluster, set_party
from fed.barriers import start_recv_proxy


@fed.remote
class MyActor:
    def __init__(self, party, data):
        self.__data = data
        self._party = party
 
    def f(self):
        print(f"=====THIS IS F IN PARTY {self._party}")
        return f"f({self._party}, ip is {ray.util.get_node_ip_address()})"

    def g(self, obj):
        print(f"=====THIS IS G IN PARTY {self._party}")
        return obj + "g"

    def h(self, obj):
        print(f"=====THIS IS H IN PARTY {self._party}, obj is {obj}")
        return obj + "h"


@fed.remote
def agg_fn(obj1, obj2):
    print(f"=====THIS IS AGG_FN, obj1={obj1}, obj2={obj2}")
    return f"agg-{obj1}-{obj2}"


cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}


def run(party):
    set_cluster(cluster=cluster)
    set_party(party)
    start_recv_proxy(cluster[party], party)

    print(f"Running the script in party {party}")

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.party("alice").remote1(party, ds1)
    actor_bob = MyActor.party("bob").remote1(party, ds2)

    obj_alice_f = actor_alice.f.remote1()
    obj_bob_f = actor_bob.f.remote1()

    obj_alice_g = actor_alice.g.remote1(obj_alice_f)
    obj_bob_h = actor_bob.h.remote1(obj_alice_f)

    obj = agg_fn.party("bob").remote1(obj_alice_g, obj_bob_h)
    result = fed.get(obj)
    print(f"The result in party {party} is :{result}")

    import time
    time.sleep(10000)
    ray.shutdown()


def main():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()


if __name__ == "__main__":
    main()
