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

import grpc
import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils
import fed.utils as fed_utils
from fed.proxy.grpc.grpc_proxy import GrpcSenderProxy, send_data_grpc

if compatible_utils._compare_version_strings(
    fed_utils.get_package_version('protobuf'), '4.0.0'
):
    from fed.grpc.pb4 import fed_pb2_grpc as fed_pb2_grpc
else:
    from fed.grpc.pb3 import fed_pb2_grpc as fed_pb2_grpc


class TestGrpcSenderProxy(GrpcSenderProxy):
    async def send(self, dest_party, data, upstream_seq_id, downstream_seq_id):
        dest_addr = self._addresses[dest_party]
        grpc_metadata, grpc_channel_options = self.get_grpc_config_by_party(dest_party)
        if dest_party not in self._stubs:
            channel = grpc.aio.insecure_channel(dest_addr, options=grpc_channel_options)
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
    'alice': '127.0.0.1:11012',
    'bob': '127.0.0.1:11011',
}


def run(party, job_name):
    ray.init(address='local', include_dashboard=False)
    fed.init(
        addresses=addresses,
        party=party,
        job_name=job_name,
        sender_proxy_cls=TestGrpcSenderProxy,
        config={
            'cross_silo_comm': {
                'exit_on_sending_failure': True,
            }
        },
    )
    # 'bob' only needs to start the proxy actors
    if party == 'alice':
        ds1, ds2 = [123, 789]
        actor_alice = MyActor.party("alice").remote(party, ds1)
        actor_bob = MyActor.party("bob").remote(party, ds2)

        obj_alice_f = actor_alice.f.remote()
        obj_bob_f = actor_bob.f.remote()

        obj = agg_fn.party("bob").remote(obj_alice_f, obj_bob_f)
        fed.get(obj)
        fed.shutdown()
        ray.shutdown()
        import time

        # Wait for SIGTERM as failure on sending.
        time.sleep(86400)


def test_ignore_other_job_msg():
    p_alice = multiprocessing.Process(target=run, args=('alice', 'job1'))
    p_bob = multiprocessing.Process(target=run, args=('bob', 'job2'))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
