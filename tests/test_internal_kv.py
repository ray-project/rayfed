import multiprocessing
import pytest
import ray
import fed
import time
import fed._private.compatible_utils as compatible_utils


def run(party):
    compatible_utils.init_ray("local")
    addresses = {
        'alice': {'address': '127.0.0.1:11010', 'listen_addr': '0.0.0.0:11010'},
        'bob': {'address': '127.0.0.1:11011', 'listen_addr': '0.0.0.0:11011'},
    }
    assert compatible_utils.kv is None
    fed.init(addresses=addresses, party=party)
    assert compatible_utils.kv
    assert not compatible_utils.kv.put(b"test_key", b"test_val")
    assert compatible_utils.kv.get(b"test_key") == b"test_val"

    time.sleep(5)
    fed.shutdown()

    assert compatible_utils.kv is None
    with pytest.raises(ValueError):
        # Make sure the kv actor is non-exist no matter whether it's in client mode
        ray.get_actor("_INTERNAL_KV_ACTOR")
    ray.shutdown()


def test_kv_init():
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
