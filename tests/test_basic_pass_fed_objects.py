import multiprocessing
import os
import test_utils
from test_utils import use_tls, build_env
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


def run(party, is_inner_party, env):
    os.environ = env
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
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


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_pass_fed_objects_for_actor_creation_inner_party(use_tls):
    p_alice = multiprocessing.Process(target=run, args=('alice', True, build_env()))
    p_bob = multiprocessing.Process(target=run, args=('bob', True, build_env()))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_pass_fed_objects_for_actor_creation_across_party(use_tls):
    p_alice = multiprocessing.Process(target=run, args=('alice', False))
    p_bob = multiprocessing.Process(target=run, args=('bob', False))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
