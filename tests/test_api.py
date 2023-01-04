import multiprocessing
import os
import test_utils
from test_utils import use_tls, build_env
import pytest
import fed


def run(env):
    os.environ = env
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party="alice")
    assert fed.get_cluster() == cluster
    assert fed.get_party() == "alice"
    fed.shutdown()


@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_fed_apis(use_tls):
    p_alice = multiprocessing.Process(target=run, args=(build_env(),))
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
