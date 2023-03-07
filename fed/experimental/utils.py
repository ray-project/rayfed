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

from types import List

import fed


def global_sync(party_list: List):
    """ An experimental util function that injects a synchronization point
    here to block until all parties executed the dummy remote functions.

    This function always return `True`.
    """

    @fed.remote
    def _dummy_func():
        return "ready"

    fed_obj_refs = [
        _dummy_func.party(party=party_name).remote()
            for party_name in party_list]
    fed.get(fed_obj_refs)
    return True
