import yaml
import tempfile
import os
import fed


@fed.remote(party="ALICE")
def generate_wrong_type():
    class WrongType:
        pass
    return WrongType()


@fed.remote(party="BOB")
def pass_wrong_type(d):
    return True


def run(party, config_path):    
    cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}
    fed.init(cluster=cluster, party=party)

    fed.shutdown()


def test_restricted_loads():
    config_path = "/tmp/test.yaml"
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
