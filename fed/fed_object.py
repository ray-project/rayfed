from typing import List
from ray import ObjectRef


class FedObject():
    """ The class that represents for a fed object handle for the result
    of the return value from a fed task.
    """
    def __init__(self, node_party: str, fed_task_id: int, object_ref: ObjectRef) -> None:
        # The party name to exeute the task which produce this fed object.
        self._node_party = node_party
        self._object_ref = object_ref
        self._fed_task_id = fed_task_id

    def get_ray_object_ref(self):
        return self._object_ref

    def get_fed_task_id(self):
        return self._fed_task_id
        
    def get_party(self):
        return self._node_party
