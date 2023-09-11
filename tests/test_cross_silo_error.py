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

import ray
import multiprocessing

import pytest
import fed
import fed._private.compatible_utils as compatible_utils
import sys

from unittest.mock import Mock
from fed._private.exceptions import RemoteError


class MyError(Exception):
    def __init__(self, message):
        super().__init__(message)


@fed.remote
def error_func():
    raise MyError("Test normal task Error")


@fed.remote
class My:
    def __init__(self) -> None:
        pass

    def error_func(self):
        raise MyError("Test actor task Error")


def run(party):
    my_failure_handler = Mock()
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }

    fed.init(
        addresses=addresses,
        party=party,
        logging_level='debug',
        config={
            'cross_silo_comm': {
                'exit_on_sending_failure': True,
                'timeout_ms': 20 * 1000,
            },
        },
        failure_handler=my_failure_handler
    )

    # Both party should catch the error
    o = error_func.party("alice").remote()
    with pytest.raises(Exception) as e:
        fed.get(o)
    if party == 'bob':
        assert isinstance(e.value.cause, RemoteError)
        assert 'RemoteError occurred at alice' in str(e.value.cause)
        assert "normal task Error" in str(e.value.cause)
    else:
        assert isinstance(e.value.cause, MyError)
        assert "normal task Error" in str(e.value.cause)
    my_failure_handler.assert_called_once()
    fed.shutdown()
    ray.shutdown()


def test_cross_silo_normal_task_error():
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0
    assert p_bob.exitcode == 0


def run2(party):
    my_failure_handler = Mock()
    compatible_utils.init_ray(address='local')
    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(
        addresses=addresses,
        party=party,
        logging_level='debug',
        config={
            'cross_silo_comm': {
                'exit_on_sending_failure': True,
                'timeout_ms': 20 * 1000,
            },
        },
        failure_handler=my_failure_handler
    )

    # Both party should catch the error
    my = My.party('alice').remote()
    o = my.error_func.remote()
    with pytest.raises(Exception) as e:
        fed.get(o)

    if party == 'bob':
        assert isinstance(e.value.cause, RemoteError)
        assert 'RemoteError occurred at alice' in str(e.value.cause)
        assert "actor task Error" in str(e.value.cause)
        my_failure_handler.assert_called_once()
    else:
        assert isinstance(e.value.cause, MyError)
        assert "actor task Error" in str(e.value.cause)
        my_failure_handler.assert_called_once()

    fed.shutdown()
    ray.shutdown()


def test_cross_silo_actor_task_error():
    p_alice = multiprocessing.Process(target=run2, args=('alice',))
    p_bob = multiprocessing.Process(target=run2, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()
    assert p_alice.exitcode == 0
    assert p_bob.exitcode == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
