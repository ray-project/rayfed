import multiprocessing
import pytest
import fed
import ray


@fed.remote
def dummpy():
    return 2


def party_grpc_options(party):
    cluster = {
        'alice': {
            'address': '127.0.0.1:11010',
            'grpc_channel_option': [
                ('grpc.default_authority', 'alice'),
                ('grpc.max_send_message_length', 51 * 1024 * 1024)
            ]},
        'bob': {
            'address': '127.0.0.1:11011',
            'grpc_channel_option': [
                ('grpc.default_authority', 'bob'),
                ('grpc.max_send_message_length', 50 * 1024 * 1024)
            ]},
    }
    fed.init(
        address='local',
        cluster=cluster,
        party=party,
        cross_silo_messages_max_size_in_bytes=100
    )

    def _assert_on_proxy(proxy_actor):
        cluster_info = ray.get(proxy_actor._get_cluster_info.remote())
        assert cluster_info['alice'] is not None
        assert cluster_info['alice']['grpc_channel_option'] is not None
        alice_channel_options = cluster_info['alice']['grpc_channel_option']
        assert ('grpc.default_authority', 'alice') in alice_channel_options
        assert ('grpc.max_send_message_length', 51 * 1024 * 1024) in alice_channel_options # noqa

        assert cluster_info['bob'] is not None
        assert cluster_info['bob']['grpc_channel_option'] is not None
        bob_channel_options = cluster_info['bob']['grpc_channel_option']
        assert ('grpc.default_authority', 'bob') in bob_channel_options
        assert ('grpc.max_send_message_length', 50 * 1024 * 1024) in bob_channel_options # noqa

    send_proxy = ray.get_actor("SendProxyActor")
    _assert_on_proxy(send_proxy)

    a = dummpy.party('alice').remote()
    b = dummpy.party('bob').remote()
    fed.get([a, b])

    fed.shutdown()


def test_party_specific_grpc_options():
    p_alice = multiprocessing.Process(target=party_grpc_options, args=('alice',))
    p_bob = multiprocessing.Process(target=party_grpc_options, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0 and p_bob.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
