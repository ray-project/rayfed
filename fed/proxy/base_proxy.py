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

import abc
from typing import Dict

from fed.config import CrossSiloMessageConfig


class SenderProxy(abc.ABC):
    def __init__(
        self,
        addresses: Dict,
        party: str,
        tls_config: Dict,
        proxy_config: CrossSiloMessageConfig = None
    ) -> None:
        self._addresses = addresses
        self._party = party
        self._tls_config = tls_config
        self._proxy_config = proxy_config

    @abc.abstractmethod
    async def send(
        self,
        dest_party,
        data,
        upstream_seq_id,
        downstream_seq_id
    ):
        pass

    async def is_ready(self):
        return True

    async def get_proxy_config(self, dest_party=None):
        return self._proxy_config


class ReceiverProxy(abc.ABC):
    def __init__(
            self,
            listen_addr: str,
            party: str,
            tls_config: Dict,
            proxy_config: CrossSiloMessageConfig = None
    ) -> None:
        self._listen_addr = listen_addr
        self._party = party
        self._tls_config = tls_config
        self._proxy_config = proxy_config

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    async def get_data(
            self,
            src_party,
            upstream_seq_id,
            curr_seq_id):
        pass

    async def is_ready(self):
        return True

    async def get_proxy_config(self):
        return self._proxy_config
