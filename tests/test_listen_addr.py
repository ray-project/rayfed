import multiprocessing

import pytest
import fed


@fed.remote
def f():
    return 100


@fed.remote
class My:
    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value


def run(party, is_inner_party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010', 'listen_addr': '0.0.0.0:11010'},
        'bob': {'address': '127.0.0.1:11011', 'listen_addr': '0.0.0.0:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    o = f.party("alice").remote()
    actor_location = "alice" if is_inner_party else "bob"
    my = My.party(actor_location).remote(o)
    val = my.get_value.remote()
    result = fed.get(val)
    assert result == 100
    assert fed.get(o) == 100
    import time

    time.sleep(5)
    fed.shutdown()


def test_listen_addr():
    p_alice = multiprocessing.Process(target=run, args=('alice', True))
    p_bob = multiprocessing.Process(target=run, args=('bob', True))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
