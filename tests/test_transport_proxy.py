# Copyright 2023 The RayFed Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cloudpickle
import pytest
import ray

import fed._private.compatible_utils as compatible_utils
from fed._private.constants import (
    KEY_OF_CLUSTER_ADDRESSES,
    KEY_OF_CLUSTER_CONFIG,
    KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES,
    KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST,
    KEY_OF_CURRENT_PARTY_NAME,
    KEY_OF_TLS_CONFIG,
    KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS,
)
from fed.barriers import send, recv, start_recv_proxy, start_send_proxy
from fed.cleanup import wait_sending


def test_n_to_1_transport():
    """This case is used to test that we have N send_op barriers,
    sending data to the target recver proxy, and there also have
    N receivers to `get_data` from Recver proxy at that time.
    """
    compatible_utils.init_ray(address='local')

    cluster_config = {
        KEY_OF_CLUSTER_ADDRESSES: "",
        KEY_OF_CURRENT_PARTY_NAME: "",
        KEY_OF_TLS_CONFIG: "",
        KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES: None,
        KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST: {},
        KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS: 60,
    }
    compatible_utils.kv.put(KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    start_recv_proxy(
        cluster_config,
        party,
        logging_level='info',
    )
    start_send_proxy(cluster_config, party, logging_level='info')

    sent_objs = []
    get_objs = []
    recver_proxy_actor = ray.get_actor(f"RecverProxyActor-{party}")
    for i in range(NUM_DATA):
        sent_obj = send(party, f"data-{i}", i, i + 1)
        sent_objs.append(sent_obj)
        get_obj = recver_proxy_actor.get_data.remote(party, i, i + 1)
        get_objs.append(get_obj)
    for result in ray.get(sent_objs):
        assert result

    for i in range(NUM_DATA):
        assert f"data-{i}" in map(lambda x: x[0], ray.get(get_objs))

    wait_sending()
    ray.shutdown()


def test_send_with_meta():
    """ This case is to test send & recv with meta-data. 
    The meta-data can either be in the form of dict: {key: value},
    or two dimensional tuple: ((key1, value1), (key2, value2)).
    """
    compatible_utils.init_ray(address='local')

    cluster_config = {
        KEY_OF_CLUSTER_ADDRESSES: "",
        KEY_OF_CURRENT_PARTY_NAME: "",
        KEY_OF_TLS_CONFIG: "",
        KEY_OF_CROSS_SILO_MESSAGES_MAX_SIZE_IN_BYTES: None,
        KEY_OF_CROSS_SILO_SERIALIZING_ALLOWED_LIST: {},
        KEY_OF_CROSS_SILO_TIMEOUT_IN_SECONDS: 60,
    }
    compatible_utils.kv.put(KEY_OF_CLUSTER_CONFIG, cloudpickle.dumps(cluster_config))

    NUM_DATA = 10
    SERVER_ADDRESS = "127.0.0.1:12344"
    party = 'test_party'
    cluster_config = {'test_party': {'address': SERVER_ADDRESS}}
    start_recv_proxy(
        cluster_config,
        party,
        logging_level='debug',
    )
    start_send_proxy(cluster_config, party, logging_level='debug')

    sent_objs = []
    get_objs = []
    sent_obj = send(party, f"test data", 0, 1, (("meta-key-0", "meta-value-0"), ))
    sent_objs.append(sent_obj)
    get_obj = recv(party, party, 0, 1)
    get_objs.append(get_obj)

    for result in ray.get(sent_objs):
        assert result
    data, recv_meta = ray.get(get_objs)[0]

    assert "test data" == data
    assert "meta-key-0" in recv_meta
    assert "meta-value-0" in recv_meta['meta-key-0']
    
    # Test metadata in dict form
    sent_obj = send(party, f"test data", 1, 2, {"meta-key-1": "meta-value-1"})
    sent_objs.append(sent_obj)
    get_obj = recv(party, party, 1, 2)
    get_objs.append(get_obj)

    for result in ray.get(sent_objs):
        assert result
    data, recv_meta = ray.get(get_objs)[1]

    assert "test data" == data
    assert "meta-key-1" in recv_meta
    assert "meta-value-1" in recv_meta['meta-key-1']

    wait_sending()
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
