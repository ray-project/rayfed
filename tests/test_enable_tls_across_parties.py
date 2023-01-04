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
    cert_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert_files/alice_certs")
    ca_config = {
                "ca_cert": os.path.join(cert_dir, "cacert.pem"),
                "cert": os.path.join(cert_dir, "servercert.pem"),
                "key": os.path.join(cert_dir, "serverkey.pem"),
    }
    tls_config_alice = { "cert": ca_config, "client_certs": { "bob": ca_config }}
    tls_config_bob = { "cert": ca_config, "client_certs": { "alice": ca_config }}
    tls_config = tls_config_alice if party == "alice" else tls_config_bob

    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party, tls_config=tls_config)

    my1 = My.party("alice").remote()
    my2 = My.party("bob").remote()
    x = my1.incr.remote(10)
    y = my2.incr.remote(20)
    o = add.party("alice").remote(x, y)
    assert fed.get(o) == 30
    fed.shutdown()

@pytest.mark.parametrize("use_tls", [True], indirect=True)
def test_enable_tls_across_parties(use_tls):
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
