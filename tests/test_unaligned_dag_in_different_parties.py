import multiprocessing

import pytest
import fed


@fed.remote
def f1():
    return 100

@fed.remote
def f2():
    return 200

@fed.remote
def g(val):
    return val + 100000

def run(party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    if party == "alice":
        # WARNING: This is not allowed in real case.
        res = fed.get(f1.party("alice").remote())
        print(f"[{party}] res is {res}")

    o = f2.party("alice").remote()
    final_res = g.party("bob").remote(o)
    val = fed.get(final_res)
    print(f"[{party}] final res is {val}")
    assert val == 100200

    fed.shutdown()


def test_p():
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
