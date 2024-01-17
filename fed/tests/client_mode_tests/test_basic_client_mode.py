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
import fed._private.compatible_utils as compatible_utils
from fed._private.compatible_utils import _compare_version_strings
from fed.tests.test_utils import ray_client_mode_setup  # noqa

pytestmark = pytest.mark.skipif(
    _compare_version_strings(
        ray.__version__,
        '2.4.0',
    ),
    reason='Skip client mode when ray > 2.4.0',
)


@fed.remote
class MyModel:
    def __init__(self, party, step_length):
        self._trained_steps = 0
        self._step_length = step_length
        self._weights = 0
        self._party = party

    def train(self):
        self._trained_steps += 1
        self._weights += self._step_length
        return self._weights

    def get_weights(self):
        return self._weights

    def set_weights(self, new_weights):
        self._weights = new_weights
        return new_weights


@fed.remote
def mean(x, y):
    return (x + y) / 2


def run(party):
    import time

    if party == 'alice':
        time.sleep(1.4)

    address = (
        'ray://127.0.0.1:21012' if party == 'alice' else 'ray://127.0.0.1:21011'
    )  # noqa
    compatible_utils.init_ray(address=address)

    addresses = {
        'alice': '127.0.0.1:31012',
        'bob': '127.0.0.1:31011',
    }
    fed.init(addresses=addresses, party=party)

    epochs = 3
    alice_model = MyModel.party("alice").remote("alice", 2)
    bob_model = MyModel.party("bob").remote("bob", 4)

    all_mean_weights = []
    for epoch in range(epochs):
        w1 = alice_model.train.remote()
        w2 = bob_model.train.remote()
        new_weights = mean.party("alice").remote(w1, w2)
        result = fed.get(new_weights)
        alice_model.set_weights.remote(new_weights)
        bob_model.set_weights.remote(new_weights)
        all_mean_weights.append(result)
    assert all_mean_weights == [3, 6, 9]
    latest_weights = fed.get(
        [alice_model.get_weights.remote(), bob_model.get_weights.remote()]
    )
    assert latest_weights == [9, 9]
    fed.shutdown()
    ray.shutdown()


def test_fed_get_in_2_parties(ray_client_mode_setup):  # noqa
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
