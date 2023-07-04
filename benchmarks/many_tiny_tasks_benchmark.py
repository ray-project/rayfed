import ray
import time
import sys
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
    ray.init(address='local')

    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(cluster=cluster, party=party)

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
    print("total time (ms) = ", (time.time() - start)*1000)
    print("per task overhead (ms) =", (time.time() - start)*1000/num_calls)

    fed.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    assert len(sys.argv) == 2, 'Please run this script with party.'
    main(sys.argv[1])


