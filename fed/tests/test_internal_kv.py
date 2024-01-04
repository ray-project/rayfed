import multiprocessing
import time

import pytest
import ray
import ray.experimental.internal_kv as ray_internal_kv

import fed
import fed._private.compatible_utils as compatible_utils


def run(party):
    compatible_utils.init_ray("local")
    addresses = {
        'alice': '127.0.0.1:12012',
        'bob': '127.0.0.1:12011',
    }
    assert compatible_utils.kv is None
    fed.init(addresses=addresses, party=party, job_name="test_job_name")
    assert compatible_utils.kv
    assert not compatible_utils.kv.put("test_key", b"test_val")
    assert compatible_utils.kv.get("test_key") == b"test_val"

    # Test that a prefix key name is added under the hood.
    assert ray_internal_kv._internal_kv_get(b"test_key") is None
    assert (
        ray_internal_kv._internal_kv_get(b"RAYFED#test_job_name#test_key")
        == b"test_val"
    )

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
