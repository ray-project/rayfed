# Copyright 2022 Ant Group Co., Ltd.
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

from ray import ObjectRef


class FedObject:
    """The class that represents for a fed object handle for the result
    of the return value from a fed task.
    """

    def __init__(
        self,
        node_party: str,
        fed_task_id: int,
        object_ref: ObjectRef,
        idx_in_task: int = 0,
    ) -> None:
        # The party name to exeute the task which produce this fed object.
        self._node_party = node_party
        self._object_ref = object_ref
        self._fed_task_id = fed_task_id
        self._idx_in_task = idx_in_task

    def get_ray_object_ref(self):
        return self._object_ref

    def get_fed_task_id(self):
        return f'{self._fed_task_id}#{self._idx_in_task}'

    def get_party(self):
        return self._node_party
