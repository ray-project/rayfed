import multiprocessing

import pytest
import fed


@fed.remote
class Counter:
    def __init__(self) -> None:
        self._value = 0

    def incr(self, delta, big_data):
        self._value += delta
        return self._value

@fed.remote
class My:
    def __init__(self) -> None:
        pass

    def get_one(self):
        return 1


def run(party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    my = My.party("alice").remote()
    counter = Counter.party("bob").options(max_concurrency=100).remote()

    import numpy as np
    big_data = np.zeros(1 * 1024 * 1024)

    epochs = 10_000
    refs = []
    for _ in range(epochs):
        one = my.get_one.remote()
        refs.append(counter.incr.remote(one, big_data))

    one = my.get_one.remote()
    final_res = counter.incr.remote(one, big_data)
    print(f"[{party}]=======Final result is {fed.get(final_res)}")
    fed.shutdown()


def test_fed_get_in_2_parties():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
