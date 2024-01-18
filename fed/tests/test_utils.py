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
        f'--include-dashboard=false',
        f'--dashboard-port={dashboard_port}',
    ]
    command_str = ' '.join(command)
    try:
        _ = fed_utils.start_command(command_str)
    except RuntimeError as e:
        # As we should treat the following warning messages is ok to use.
        # E         RuntimeError: Failed to start command [ray start --head --port=41012
        # --ray-client-server-port=21012 --dashboard-port=9112], the error is:
        # E         2023-09-13 13:04:11,520	WARNING services.py:1882 -- WARNING: The
        # object store is using /tmp instead of /dev/shm because /dev/shm has only
        # 67108864 bytes available. This will harm performance! You may be able to
        # free up space by deleting files in /dev/shm. If you are inside a Docker
        # container, you can increase /dev/shm size by passing '--shm-size=1.97gb' to
        # 'docker run' (or add it to the run_options list in a Ray cluster config).
        # Make sure to set this to more than 0% of available RAM.
        assert 'Overwriting previous Ray address' in str(
            e
        ) or 'WARNING: The object store is using /tmp instead of /dev/shm' in str(e)


@pytest.fixture
def ray_client_mode_setup():
    # Start 2 Ray clusters.
    start_ray_cluster(ray_port=41012, client_server_port=21012, dashboard_port=9112)
    time.sleep(1)
    start_ray_cluster(ray_port=41011, client_server_port=21011, dashboard_port=9111)

    yield
    fed_utils.start_command('ray stop --force')
