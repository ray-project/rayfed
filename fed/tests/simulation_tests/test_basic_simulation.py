import pytest
import fed


@fed.remote
class Server:
    def __init__(self, clients) -> None:
        self._clients = clients

    def sum(self):
        sum_value = 0
        for _ in range(10):
            curr_refs = [client.incr.remote() for client in self._clients]
            curr_values = fed.get(curr_refs)
            sum_value = sum_value + sum(curr_values)
        return sum_value

@fed.remote
class Client:
    def __init__(self, original_value) -> None:
        self._value = original_value
    
    def incr(self):
        self._value = self._value + 1
        return self._value


def test_simple_clients():
    fed.init(sim_mode=True)

    s = Server.remote()
    NUM = 2
    clients = [Client.remote(i) for i in range(NUM)]
    s = Server.remote(clients)
    final_result_ref = s.sum.remote()
    final_result = fed.get(final_result_ref)
    assert final_result == 100


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
