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

from ray import ObjectRef


class FedObjectSendingContext:
    """The class that's used for holding the all contexts about sending side."""
    def __init__(self) -> None:
        # This field holds the target(downstream) parties that this fed object
        # is sending or sent to.
        # The key is the party name and the value is a boolean indicating whether
        # this object is sending or sent to the party.
        self._is_sending_or_sent = {}

    def mark_is_sending_to_party(self, target_party: str):
        self._is_sending_or_sent[target_party] = True

    def was_sending_or_sent_to_party(self, target_party: str):
        return target_party in self._is_sending_or_sent


class FedObjectReceivingContext:
    """The class that's used for holding the all contexts about receiving side."""
    pass


class FedObject:
    """The class that represents for a fed object handle for the result
    of the return value from a fed task.
    """

    def __init__(
        self,
        node_party: str,
        fed_task_id: int,
        ray_object_ref: ObjectRef,
        idx_in_task: int = 0,
    ) -> None:
        # The party name to exeute the task which produce this fed object.
        self._node_party = node_party
        self._ray_object_ref = ray_object_ref
        self._fed_task_id = fed_task_id
        self._idx_in_task = idx_in_task
        self._sending_context = FedObjectSendingContext()
        self._receiving_context = FedObjectReceivingContext()

    def get_ray_object_ref(self):
        return self._ray_object_ref

    def get_fed_task_id(self):
        return f'{self._fed_task_id}#{self._idx_in_task}'

    def get_party(self):
        return self._node_party

    def _mark_is_sending_to_party(self, target_party: str):
        """Mark this fed object is sending to the target party."""
        self._sending_context.mark_is_sending_to_party(target_party)

    def _was_sending_or_sent_to_party(self, target_party: str):
        """Query whether this fed object was sending or sent to the target party."""
        return self._sending_context.was_sending_or_sent_to_party(target_party)

    def _cache_ray_object_ref(self, ray_object_ref):
        """Cache the ray object reference for this fed object."""
        self._ray_object_ref = ray_object_ref
