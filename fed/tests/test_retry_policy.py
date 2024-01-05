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
from unittest import TestCase

import pytest
import ray

import fed
import fed._private.compatible_utils as compatible_utils
from fed import config


@fed.remote
def f():
    return 100


@fed.remote
class My:
    def __init__(self, value) -> None:
        self._value = value

    def get_value(self):
        return self._value


def run():
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    retry_policy = {
        "maxAttempts": 4,
        "initialBackoff": "5s",
        "maxBackoff": "5s",
        "backoffMultiplier": 1,
        "retryableStatusCodes": ["UNAVAILABLE"],
    }
    test_job_name = 'test_retry_policy'
    fed.init(
        addresses=addresses,
        party='alice',
        config={
            'cross_silo_comm': {
                'grpc_retry_policy': retry_policy,
            }
        },
    )

    job_config = config.get_job_config(test_job_name)
    cross_silo_comm_config = job_config.cross_silo_comm_config_dict
    TestCase().assertDictEqual(
        cross_silo_comm_config['grpc_retry_policy'], retry_policy
    )

    fed.shutdown()
    ray.shutdown()


def test_retry_policy():
    p_alice = multiprocessing.Process(target=run)
    p_alice.start()
    p_alice.join()
    assert p_alice.exitcode == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
