import pytest

import fed
import ray

@fed.remote
def f():
    return 100

@fed.remote
class A:
    def hello(self):
        return "hello"

    def world(self):
        return "world"

@fed.remote
def agg(x, y):
    return f"{x}-{y}"

def test_decorator_api():
    ray.init()
    a = A.party("BOB").bind()

    hello_o = a.hello.bind()
    world_o = a.world.bind()
    f_o = f.party("ALICE").bind()
    agg_o = agg.party("ALICE").bind(hello_o, f_o)
    assert ray.get(agg_o.execute()) == "hello-100"
    ray.shutdown()

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
