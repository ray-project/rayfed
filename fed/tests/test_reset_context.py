import multiprocessing

import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils

addresses = {
    'alice': '127.0.0.1:11012',
    'bob': '127.0.0.1:11011',
}


@fed.remote
class A:
    def __init__(self, init_val=0) -> None:
        self.value = init_val

    def get(self):
        return self.value


def run(party):
    compatible_utils.init_ray(address='local')
    fed.init(addresses=addresses, party=party)

    actor = A.party('alice').remote(10)
    alice_fed_obj = actor.get.remote()
    alice_first_fed_obj_id = alice_fed_obj.get_fed_task_id()
    assert fed.get(alice_fed_obj) == 10

    actor = A.party('bob').remote(12)
    bob_fed_obj = actor.get.remote()
    bob_first_fed_obj_id = bob_fed_obj.get_fed_task_id()
    assert fed.get(bob_fed_obj) == 12

    assert compatible_utils.kv.put("key", "val") is False
    assert compatible_utils.kv.get("key") == b"val"
    fed.shutdown()
    ray.shutdown()
    with pytest.raises(AttributeError):
        # `internal_kv` should be reset, putting to which should raise
        # `AttributeError`
        compatible_utils.kv.put("key2", "val2")

    compatible_utils.init_ray(address='local')
    fed.init(addresses=addresses, party=party)

    actor = A.party('alice').remote(10)
    alice_fed_obj = actor.get.remote()
    alice_second_fed_obj_id = alice_fed_obj.get_fed_task_id()
    assert fed.get(alice_fed_obj) == 10
    assert alice_first_fed_obj_id == alice_second_fed_obj_id

    actor = A.party('bob').remote(12)
    bob_fed_obj = actor.get.remote()
    bob_second_fed_obj_id = bob_fed_obj.get_fed_task_id()
    assert fed.get(bob_fed_obj) == 12
    assert bob_first_fed_obj_id == bob_second_fed_obj_id

    assert compatible_utils.kv.get("key") is None
    assert compatible_utils.kv.put("key", "val") is False
    assert compatible_utils.kv.get("key") == b"val"

    fed.shutdown()
    ray.shutdown()


def test_reset_context():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()

    import time

    time.sleep(5)
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
