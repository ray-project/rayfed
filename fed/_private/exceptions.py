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

class RemoteError(Exception):
    def __init__(self, src_party: str, cause: Exception) -> None:
        self._src_party = src_party
        self._cause = cause

    def __str__(self):
        return f'RemoteError occurred at {self._src_party} caused by {str(self._cause)}'
