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

import time
import pytest

import fed.utils as fed_utils


def start_ray_cluster(
        ray_port,
        client_server_port,
        dashboard_port,
):
    command = [
        'ray',
        'start',
        '--head',
        f'--port={ray_port}',
        f'--ray-client-server-port={client_server_port}',
        f'--dashboard-port={dashboard_port}',
    ]
    command_str = ' '.join(command)
    _ = fed_utils.start_command(command_str)


@pytest.fixture
def ray_client_mode_setup():
    # Start 2 Ray clusters.
    start_ray_cluster(ray_port=41012, client_server_port=21012, dashboard_port=9112)

    time.sleep(1)
    try:
        start_ray_cluster(ray_port=41011, client_server_port=21011, dashboard_port=9111)
    except RuntimeError as e:
        # A successful case.
        assert 'Overwriting previous Ray address' in str(e)

    yield
    fed_utils.start_command('ray stop --force')
