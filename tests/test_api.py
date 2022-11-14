import multiprocessing
import pytest
import fed


def run():
    cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}
    fed.init(cluster=cluster, party="alice")
    assert fed.get_cluster() == cluster
    assert fed.get_party() == "alice"
    fed.shutdown()

def test_fed_apis():
    p_alice = multiprocessing.Process(target=run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
