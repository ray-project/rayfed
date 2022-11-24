import os
import pytest

import ray
import cloudpickle
import ray.experimental.internal_kv as internal_kv

from fed.barriers import RecverProxyActor, send
from fed._private.constants import RAYFED_TLS_CONFIG


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target recver proxy, and there also have
    N receivers to `get_data` from Recver proxy at that time.
    """
    ray.init()

    cert_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "cert_files/alice_certs"
    )
    ca_config = {
        "ca_cert": os.path.join(cert_dir, "cacert.pem"),
        "cert": os.path.join(cert_dir, "servercert.pem"),
        "key": os.path.join(cert_dir, "serverkey.pem"),
    }
    tls_config = {"cert": ca_config, "client_certs": {"test_node_party": ca_config}}
    internal_kv._internal_kv_put(RAYFED_TLS_CONFIG, cloudpickle.dumps(tls_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:65422"
    recver_proxy_actor = RecverProxyActor.options(
        name=f"RecverProxyActor-TEST", max_concurrency=2000
    ).remote(SERVER_ADDRESS, "test_party", tls_config)
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())

    sent_objs = []
    get_objs = []
    for i in range(NUM_DATA):
        sent_obj = send(
            "test_party",
            SERVER_ADDRESS,
            f"data-{i}",
            i,
            i + 1,
            tls_config,
            "test_node_party",
        )
        sent_objs.append(sent_obj)
        get_obj = recver_proxy_actor.get_data.remote(i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in ray.get(get_objs)
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
