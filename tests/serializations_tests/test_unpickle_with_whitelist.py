import yaml
import tempfile
import os
import fed
import multiprocessing
import numpy
import ray


@fed.remote
def generate_wrong_type():
    class WrongType:
        pass

    return WrongType()


@fed.remote
def generate_allowed_type():
    return numpy.array([1, 2, 3, 4, 5])


@fed.remote
def pass_arg(d):
    return True


def run(party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    allowedlist =  {
                "numpy.core.numeric": ["*"],
                "numpy": ["dtype"],
    }
    fed.init(
        address='local',
        cluster=cluster,
        party=party,
        cross_silo_serializing_allowed_list=allowedlist)

    # Test passing an allowed type.
    o1 = generate_allowed_type.party("alice").remote()
    o2 = pass_arg.party("bob").remote(o1)
    res = fed.get(o2)
    assert res

    # Test passing an unallowed type.
    o3 = generate_wrong_type.party("alice").remote()
    o4 = pass_arg.party("bob").remote(o3)
    if party == "bob":
        try:
            fed.get(o4)
            assert False, "This code path shouldn't be reached."
        except Exception as e:
            assert "_pickle.UnpicklingError" in str(e)
    else:
        import time

        time.sleep(10)
    fed.shutdown()


def test_restricted_loads():
    with tempfile.TemporaryDirectory() as tmp_dir:
        whitelist_config = {
            "pickle_whitelist": {
                "numpy.core.numeric": ["*"],
                "numpy": ["dtype"],
            }
        }
        yaml.safe_dump(whitelist_config, open(config_path, "wt"))

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
