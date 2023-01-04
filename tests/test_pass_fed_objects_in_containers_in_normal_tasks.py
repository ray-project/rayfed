import test_utils
from test_utils import use_tls, build_env
import os
import multiprocessing

import pytest

import fed


@fed.remote
def foo(i: int):
    return f"foo-{i}"

@fed.remote
def bar(li):
    assert li[0] == "hello"
    li1 = li[1]
    li2 = li[2]
    assert fed.get(li1[0]) == "foo-0"
    assert li2[0] == "world"
    assert fed.get(li2[1][0]) == "foo-1"
    return True

cluster = {
    'alice': {'address': '127.0.0.1:11010'},
    'bob': {'address': '127.0.0.1:11011'},
}


def run(party, env):
    os.environ = env
    fed.init(address='local', cluster=cluster, party=party)
    o1 = foo.party("alice").remote(0)
    o2 = foo.party("bob").remote(1)
    li = ["hello", [o1], ["world", [o2]]]
    o3 = bar.party("bob").remote(li)

    result = fed.get(o3)
    assert result
    fed.shutdown()


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_pass_fed_objects_in_list(use_tls):
    p_alice = multiprocessing.Process(target=run, args=('alice', build_env()))
    p_bob = multiprocessing.Process(target=run, args=('bob', build_env()))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
