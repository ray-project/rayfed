import pytest
import fed


def test_fed_apis():
    cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}
    fed.init(cluster=cluster, party="alice")
    assert fed.get_cluster() == cluster
    assert fed.get_party() == "alice"
    fed.shutdown()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
