import multiprocessing
import pytest
import ray
import fed
import time
import fed._private.compatible_utils as compatible_utils


def test_kv_init():
    def run(party):
        compatible_utils.init_ray("local")
        cluster = {
            'alice': {'address': '127.0.0.1:11010', 'listen_addr': '0.0.0.0:11010'},
            'bob': {'address': '127.0.0.1:11011', 'listen_addr': '0.0.0.0:11011'},
        }
        assert compatible_utils.kv is None
        fed.init(cluster=cluster, party=party)
        assert compatible_utils.kv
        assert not compatible_utils.kv.put(b"test_key", b"test_val")
        assert compatible_utils.kv.get(b"test_key") == b"test_val"

        time.sleep(5)
        fed.shutdown()
        ray.shutdown()

        assert compatible_utils.kv is None
        with pytest.raises(ValueError):
            ray.get_actor("_INTERNAL_KV_ACTOR")

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
