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
        invoking_frame: list = None,
    ) -> None:
        # The party name to exeute the task which produce this fed object.
        self._node_party = node_party
        self._object_ref = object_ref
        self._fed_task_id = fed_task_id
        self._idx_in_task = idx_in_task
        assert invoking_frame is not None # TODO(qwang): this should be refined.
        self._invoking_frame = invoking_frame

    def get_ray_object_ref(self):
        return self._object_ref

    def get_fed_task_id(self):
        return f'{self._fed_task_id}#{self._idx_in_task}'

    def get_party(self):
        return self._node_party

    def get_invoking_frame(self):
        return self._invoking_frame
