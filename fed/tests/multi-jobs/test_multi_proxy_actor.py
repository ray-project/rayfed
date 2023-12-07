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

import multiprocessing
import fed
import ray
import grpc
import pytest
import fed.utils as fed_utils
import fed._private.compatible_utils as compatible_utils
from fed.proxy.grpc.grpc_proxy import GrpcSenderProxy, send_data_grpc
if compatible_utils._compare_version_strings(
        fed_utils.get_package_version('protobuf'), '4.0.0'):
    from fed.grpc.pb4 import fed_pb2_grpc as fed_pb2_grpc
else:
    from fed.grpc.pb3 import fed_pb2_grpc as fed_pb2_grpc


class TestGrpcSenderProxy(GrpcSenderProxy):
    async def send(
            self,
            dest_party,
            data,
            upstream_seq_id,
            downstream_seq_id):
        dest_addr = self._addresses[dest_party]
        grpc_metadata, grpc_channel_options = self.get_grpc_config_by_party(dest_party)
        if dest_party not in self._stubs:
            channel = grpc.aio.insecure_channel(
                dest_addr, options=grpc_channel_options)
            stub = fed_pb2_grpc.GrpcServiceStub(channel)
            self._stubs[dest_party] = stub

        timeout = self._proxy_config.timeout_in_ms / 1000
        response: str = await send_data_grpc(
            data=data,
            stub=self._stubs[dest_party],
            upstream_seq_id=upstream_seq_id,
            downstream_seq_id=downstream_seq_id,
            job_name=self._job_name,
            timeout=timeout,
            metadata=grpc_metadata,
        )
        assert response.code == 417
        assert "JobName mis-match" in response.result
        # So that process can exit
        raise RuntimeError(response.result)


@fed.remote
class MyActor:
    def __init__(self, party, data):
        self.__data = data
        self._party = party

    def f(self):
        return f"f({self._party}, ip is {ray.util.get_node_ip_address()})"


@fed.remote
def agg_fn(obj1, obj2):
    return f"agg-{obj1}-{obj2}"


addresses = {
    'job1': {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    },
    'job2': {
        'alice': '127.0.0.1:12012',
        'bob': '127.0.0.1:12011',
    },
}


def run(party, job_name):
    ray.init(address='local')
    fed.init(addresses=addresses[job_name],
             party=party,
             job_name=job_name,
             sender_proxy_cls=TestGrpcSenderProxy,
             config={
                'cross_silo_comm': {
                    'exit_on_sending_failure': True,
                    'use_global_proxy': False
                }})

    sender_proxy_actor_name = f"SenderProxyActor_{job_name}"
    receiver_proxy_actor_name = f"ReceiverProxyActor_{job_name}"
    assert ray.get_actor(sender_proxy_actor_name)
    assert ray.get_actor(receiver_proxy_actor_name)

    fed.shutdown()
    ray.shutdown()


def test_multi_proxy_actor():
    p_alice_job1 = multiprocessing.Process(target=run, args=('alice', 'job1'))
    p_alice_job2 = multiprocessing.Process(target=run, args=('alice', 'job2'))
    p_alice_job1.start()
    p_alice_job2.start()
    p_alice_job1.join()
    p_alice_job2.join()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
