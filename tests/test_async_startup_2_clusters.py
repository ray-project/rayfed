import multiprocessing
import os
import test_utils
from test_utils import use_tls, build_env
import pytest

import fed


@fed.remote
class My:
    def __init__(self) -> None:
        self._val = 0

    def incr(self, delta):
        self._val += delta
        return self._val


@fed.remote
def add(x, y):
    return x + y


def _run(party: str, env):
    os.environ = env
    if party == "alice":
        import time

        time.sleep(10)

    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    my1 = My.party("alice").remote()
    my2 = My.party("bob").remote()
    x = my1.incr.remote(10)
    y = my2.incr.remote(20)
    o = add.party("alice").remote(x, y)
    assert 30 == fed.get(o)
    fed.shutdown()


# This case is used to test that we start 2 clusters not at the same time.
@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_async_startup_2_clusters(use_tls):
    p_alice = multiprocessing.Process(target=_run, args=('alice', build_env()))
    p_bob = multiprocessing.Process(target=_run, args=('bob', build_env()))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
