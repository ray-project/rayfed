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
import grpc
import pytest
import ray
import multiprocessing

import fed._private.compatible_utils as compatible_utils
import fed.utils as fed_utils
from fed.proxy.barriers import ReceiverProxyActor
from fed.proxy.grpc.grpc_proxy import GrpcReceiverProxy

if compatible_utils._compare_version_strings(
    fed_utils.get_package_version('protobuf'), '4.0.0'
):
    from fed.grpc.pb4 import fed_pb2 as fed_pb2
    from fed.grpc.pb4 import fed_pb2_grpc as fed_pb2_grpc
else:
    from fed.grpc.pb3 import fed_pb2 as fed_pb2
    from fed.grpc.pb3 import fed_pb2_grpc as fed_pb2_grpc


def run():
    # GIVEN
    ray.init(address='local', include_dashboard=False)
    address = '127.0.0.1:15111'
    receiver_proxy_actor = ReceiverProxyActor.remote(
        listening_address=address,
        party='alice',
        job_name='job1',
        logging_level='info',
        proxy_cls=GrpcReceiverProxy,
    )
    receiver_proxy_actor.start.remote()
    server_state = ray.get(receiver_proxy_actor.is_ready.remote(), timeout=60)
    assert server_state[0], server_state[1]

    # WHEN
    channel = grpc.insecure_channel(address)
    stub = fed_pb2_grpc.GrpcServiceStub(channel)

    data = cloudpickle.dumps('data')
    request = fed_pb2.SendDataRequest(
        data=data,
        upstream_seq_id=str(1),
        downstream_seq_id=str(2),
        job_name='job2',
    )
    response = stub.SendData(request)

    # THEN
    assert response.code == 417
    assert "JobName mis-match" in response.result

    ray.shutdown()


def test_ignore_other_job_msg():
    p_alice = multiprocessing.Process(target=run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
