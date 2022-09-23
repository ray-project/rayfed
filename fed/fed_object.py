from typing import List
from ray import ObjectRef


class FedObject():
    """ The class that represents for an object handle for the result
    of the return value from a fed task.

    It holds the all ray object references that need to be drived in this
    party.
    """
    def __init__(self, object_refs: List[ObjectRef]) -> None:
        # The ray object references that are produced from the ray tasks
        # which are drived in this party.
        self._object_refs = object_refs
    
    def get_ray_object_refs(self):
        return self._object_refs
