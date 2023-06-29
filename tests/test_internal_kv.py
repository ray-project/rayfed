import multiprocessing
import pytest
import ray
import fed
import time
import fed._private.compatible_utils as compatible_utils



def start_ray(port=10001):
    import socket
    import subprocess
    hostname = socket.gethostname()
    ip_addresses = []
    for addrinfo in socket.getaddrinfo(hostname, None):
        ip_addresses.append(addrinfo[4][0])
    assert len(set(ip_addresses)) > 0
    node_ip = list(set(ip_addresses))[0]
    subprocess.run(["ray", "start",
                      "--head",
                      "--ray-client-server-port", str(port),
                      "--node-ip-address", node_ip], check=True)
    return node_ip, port


def stop_ray():
    import subprocess
    subprocess.run(["ray", "stop", "-f"], check=True)


def test_client_kv_accessing():
    def run(party, ray_addr):
        compatible_utils.init_ray(ray_addr)
        cluster = {
            'alice': {'address': '127.0.0.1:11012', 'listen_addr': '0.0.0.0:11012'},
            'bob': {'address': '127.0.0.1:11011', 'listen_addr': '0.0.0.0:11011'},
        }
        assert compatible_utils.kv is None
        fed.init(cluster=cluster, party=party)
        assert compatible_utils.kv
        # "False" to not exist before
        assert not compatible_utils.kv.put(b"test_key", b"test_val")
        assert compatible_utils.kv.get(b"test_key") == b"test_val"
        assert compatible_utils.kv.delete(b"test_key") == 1  # delete num
        assert not compatible_utils.kv.get(b"test_key")

        assert not compatible_utils.kv.put(b"test_key_2", b"test_val")
        fed.shutdown()
        assert compatible_utils.kv is None
        # Accessing KV via Ray to test whether fed data is cleared
        assert not ray.util.client.ray._internal_kv_get(b"test_key_2")
        ray.shutdown() 
        stop_ray()

    node_ip, port = start_ray(10001)
    ray_addr = f"ray://{node_ip}:{port}"

    p_alice = multiprocessing.Process(target=run, args=('alice', ray_addr))
    p_bob = multiprocessing.Process(target=run, args=('bob', ray_addr))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0  


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
        assert compatible_utils.kv.delete(b"test_key") == 1  # delete num

        time.sleep(5)
        fed.shutdown()
        ray.shutdown()

        assert compatible_utils.kv is None

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
