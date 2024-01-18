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

import multiprocessing

import ray

import fed


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


addresses = {
    'alice': '127.0.0.1:11012',
    'bob': '127.0.0.1:11011',
}


def run(party):
    ray.init(address='local', include_dashboard=False)
    fed.init(addresses=addresses, party=party)
    print(f"Running the script in party {party}")

    ds1, ds2 = [123, 789]
    actor_alice = MyActor.party("alice").remote(party, ds1)
    actor_bob = MyActor.party("bob").remote(party, ds2)

    obj_alice_f = actor_alice.f.remote()
    obj_bob_f = actor_bob.f.remote()

    obj_alice_g = actor_alice.g.remote(obj_alice_f)
    obj_bob_h = actor_bob.h.remote(obj_bob_f)

    obj = agg_fn.party("bob").remote(obj_alice_g, obj_bob_h)
    result = fed.get(obj)
    print(f"The result in party {party} is :{result}")
    fed.shutdown()
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
