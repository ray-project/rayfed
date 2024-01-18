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
import pytest
import ray

import fed
from fed.proxy.barriers import receiver_proxy_actor_name, sender_proxy_actor_name
from fed.proxy.grpc.grpc_proxy import GrpcSenderProxy


def run():
    job_name = 'job_test'
    ray.init(address='local', include_dashboard=False)
    fed.init(
        addresses={
            'alice': '127.0.0.1:11012',
        },
        party='alice',
        job_name=job_name,
        sender_proxy_cls=GrpcSenderProxy,
        config={
            'cross_silo_comm': {
                'exit_on_sending_failure': True,
                # Create unique proxy for current job
                'use_global_proxy': False,
            }
        },
    )

    assert ray.get_actor(sender_proxy_actor_name())
    assert ray.get_actor(receiver_proxy_actor_name())

    fed.shutdown()
    ray.shutdown()


def test_multi_proxy_actor():
    p_alice = multiprocessing.Process(target=run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
