import multiprocessing

import pytest
import fed


@fed.remote
class Foo:
    def run(self):
        return 2, 3

@fed.remote
def bar(x):
    return x / 2, x * 2


def run(party):    
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    foo = Foo.party("alice").remote()
    a, b = fed.get(foo.run.options(num_returns=2).remote())
    c, d = fed.get(bar.party("bob").options(num_returns=2).remote(2))

    assert a == 2 and b == 3
    assert c == 1 and d == 4
    
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
