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

import sys
import time

import ray

import fed


@fed.remote
class MyActor:
    def run(self):
        return None


@fed.remote
class Aggregator:
    def aggr(self, val1, val2):
        return None


def main(party):
    ray.init(address='local', include_dashboard=False)

    addresses = {
        'alice': '127.0.0.1:11010',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party)

    actor_alice = MyActor.party("alice").remote()
    actor_bob = MyActor.party("bob").remote()
    aggregator = Aggregator.party("alice").remote()

    start = time.time()
    num_calls = 10000
    for i in range(num_calls):
        val_alice = actor_alice.run.remote()
        val_bob = actor_bob.run.remote()
        sum_val_obj = aggregator.aggr.remote(val_alice, val_bob)
        fed.get(sum_val_obj)
        if i % 100 == 0:
            print(f"Running {i}th call")
    print(f"num calls: {num_calls}")
    print("total time (ms) = ", (time.time() - start) * 1000)
    print("per task overhead (ms) =", (time.time() - start) * 1000 / num_calls)

    fed.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    assert len(sys.argv) == 2, 'Please run this script with party.'
    main(sys.argv[1])
