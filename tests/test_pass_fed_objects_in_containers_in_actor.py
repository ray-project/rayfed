
import multiprocessing

import pytest
import fed
import ray
from fed.api import get_party, set_cluster, set_party
from fed.barriers import start_recv_proxy

import jax.tree_util as jtutil

@fed.remote
class My:

    def foo(self, i: int):
        return f"foo-{i}"

    def bar(self, li):
        assert li[0] == "hello"
        li1 = li[1]
        li2 = li[2]
        assert ray.get(li1[0]) == "foo-0"
        assert li2[0] == "world"
        assert ray.get(li2[1][0]) == "foo-1"
        return True

cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}


def run(party):
    ray.init()
    set_cluster(cluster=cluster)
    set_party(party)
    start_recv_proxy(cluster[party], party)

    my1 = My.party("alice").remote()
    my2 = My.party("bob").remote()
    o1 = my1.foo.remote(0)
    o2 = my2.foo.remote(1)
    li = ["hello", [o1], ["world", [o2]]]
    o3 = my2.bar.remote(li)

    result = fed.get(o3)
    assert result
    ray.shutdown()


def test_pass_fed_objects_in_list():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
