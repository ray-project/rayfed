import functools
import inspect
import logging
from typing import Dict

import ray
from ray._private.inspect_util import is_cython
from ray.dag import PARENT_CLASS_NODE_KEY, PREV_CLASS_METHOD_CALL_KEY
from ray.dag.class_node import ClassMethodNode, ClassNode, _UnboundClassMethodNode
from ray.dag.function_node import FunctionNode

from .barriers import send_op, recv_op

logger = logging.getLogger(__file__)


_PARTY = None


def set_party(party: str):
    global _PARTY
    _PARTY = party


def get_party():
    global _PARTY
    return _PARTY


_CLUSTER = None
'''
{
    'alice': '127.0.0.1:10001',
    'bob': '127.0.0.1:10002',
}
'''


def get_cluster():
    global _CLUSTER
    return _CLUSTER


def set_cluster(cluster: Dict):
    global _CLUSTER
    _CLUSTER = cluster


def _is_fed_dag_node(node):
    # A helper that returns thether the argument is type of FedDAGNode.
    return isinstance(
        node, (FedDAGClassNode, FedDAGClassMethodNode, FedDAGFunctionNode)
    )


def _remove_from_tuple_helper(tp, item):
    li = list(tp)
    li.remove(item)
    return tuple(li)


def _append_to_tuple_helper(tp, item):
    li = list(tp)
    li.append(item)
    return tuple(li)


def executable(cls):
    def make_exec():
        def exec(self, party: str = None):
            if not party:
                party = get_party()
            assert party, 'Must given a party name.'
            fed_dag_handle = FedDAG(self, party=party)
            fed_dag_handle.build()
            return fed_dag_handle.execute()

        setattr(cls, 'exec', exec)
        return cls

    return make_exec()


@executable
class FedDAGFunctionNode(FunctionNode):
    def __init__(self, func_body, func_args, func_kwargs, party: str):
        self._func_body = func_body
        self._party = party
        super().__init__(func_body, func_args, func_kwargs, None)

    def get_func_or_method_name(self):
        return self._func_body.__name__

    def get_party(self):
        return self._party


@executable
class FedDAGClassNode(ClassNode):
    def __init__(self, cls, party, cls_args, cls_kwargs):
        self._party = party
        self._cls = cls
        super().__init__(
            cls, cls_args, cls_kwargs, {}
        )  # TODO(qwang): Pass options instead of None.

    def __getattr__(self, method_name: str):
        # User trying to call .bind() without a bind class method
        if method_name == "bind" and "bind" not in dir(self._body):
            raise AttributeError(f".bind() cannot be used again on {type(self)} ")
        # Raise an error if the method is invalid.
        getattr(self._body, method_name)
        call_node = _UnboundFedMethodNode(self, method_name, self._party)
        return call_node

    def get_func_or_method_name(self):
        return self._cls.__name__

    def get_party(self) -> str:
        return self._party


class _UnboundFedMethodNode(_UnboundClassMethodNode):
    # rename class_node instead of actor.
    def __init__(self, actor, method_name, party: str):
        self._party = party
        super().__init__(actor, method_name)

    def bind(self, *args, **kwargs):
        other_args_to_resolve = {
            PARENT_CLASS_NODE_KEY: self._actor,
            PREV_CLASS_METHOD_CALL_KEY: self._actor._last_call,
        }

        node = FedDAGClassMethodNode(
            self._party,
            self._method_name,
            args,
            kwargs,
            self._options,
            other_args_to_resolve=other_args_to_resolve,
        )
        self._actor._last_call = node
        return node


@executable
class FedDAGClassMethodNode(ClassMethodNode):
    def __init__(
        self,
        party,
        method_name,
        method_args,
        method_kwargs,
        options,
        other_args_to_resolve=None,
    ):
        self._party = party
        self._method_name = method_name
        super().__init__(
            method_name,
            method_args,
            options,
            method_kwargs,
            other_args_to_resolve=other_args_to_resolve,
        )

    def get_func_or_method_name(self):
        return self._method_name

    def get_party(self):
        return self._party


class FedDAG:
    def __init__(self, final_fed_dag_node, cluster: dict = None, party: str = None):
        self._all_sub_dags = []
        self._final_fed_dag_node = final_fed_dag_node
        self._all_node_info = {}
        self._seq_count = 0
        self._local_dag_uuids = None
        if not cluster:
            cluster = get_cluster()
        assert cluster
        self._cluster = cluster
        print(cluster)
        if not party:
            party = get_party()
        assert party
        assert party in cluster, f'{party} not in cluster {cluster}.'
        self._party = party
        # The value is the upstream node to key. key -> set of values
        # TODO(qwang): The indexes should be changed to key -> set(),
        # because that every node may has more than 1 upstream or downstream.
        self._upstream_indexes: Dict = None
        # The value is the downstream node to key.
        self._downstream_indexes: Dict = None
        self._recver_proxy_actor_node = None
        self._injected_send_ops = None
        self._injectted_recv_ops = None
        # This maintains that a list of nodes which need to be drived to submit.
        # For example, considering the following multiple parties DAG:
        #
        #  a -> b       /--> d               ALICE
        #       \      /
        #        \--> c                      BOB
        #
        # In ALICE party, we should driver not only node `d`, we should also drive
        # `send_op` for node `b`, that's why we need to maintain a list of nodes,
        # which we need drive.
        self._nodes_need_to_drive = []

    def get_seq_id_by_uuid(self, uuid):
        seq_id, _ = self._all_node_info[uuid]
        assert seq_id is not None
        return seq_id

    def get_all_node_info(self):
        return self._all_node_info

    def get_curr_seq_count(self):
        return self._seq_count

    def get_local_dag_uuids(self):
        assert self._local_dag_uuids is not None, "Local DAG should be extracted first."
        return self._local_dag_uuids

    def __generate_all_node_info(self, fed_node):
        """This help method is used to generate the indexes from
        node uuid to the node entry and node seq id.
        """
        if not _is_fed_dag_node(fed_node):
            return

        self._all_node_info[fed_node.get_stable_uuid()] = (self._seq_count, fed_node)
        self._seq_count += 1

        child_nodes: list = fed_node._get_all_child_nodes()
        for child_node in child_nodes:
            if _is_fed_dag_node(child_node):
                self.__generate_all_node_info(child_node)

    def _build(self):
        """An internal API to build all the info of this dag."""
        self.__generate_all_node_info(self._final_fed_dag_node)

    def _extract_local_dag(self) -> None:
        assert self._local_dag_uuids is None, "This shouldn't be called twice."

        self._local_dag_uuids = []
        for uuid in self._all_node_info:
            _, node = self._all_node_info[uuid]
            options = node.get_options()
            assert options is not None
            if self._party == node.get_party():
                self._local_dag_uuids.append(uuid)

    def _inject_barriers(self) -> None:
        # Create a RecverProxy for this party.
        # assert self._recver_proxy_actor_node is None, "Barriers had been injected."
        # self._recver_proxy_actor_node = RecverProxyActor.bind(listening_addr="127.0.0.1:11122")
        # run_grpc_node = self._recver_proxy_actor_node.run_grpc_server.bind()
        # This increse 2 Proxy process there. We should fix it by use a remote actor or use the
        # same node.
        # run_grpc_node.execute()

        assert self._injected_send_ops is None
        assert self._injectted_recv_ops is None
        self._injected_send_ops = []
        self.injected_recv_ops = []

        for uuid in self._local_dag_uuids:
            _, curr_node = self._all_node_info[uuid]
            if uuid in self._upstream_indexes:
                # 将这个node的该上游去掉, 替换为RecverProxy.get_data
                # TODO(qwang): 我们暂时只处理args里的依赖，先不处理其他的.
                found_upstream_arg_nodes = []
                for arg in curr_node._bound_args:
                    if (
                        _is_fed_dag_node(arg)
                        and arg.get_stable_uuid() == self._upstream_indexes[uuid][1]
                    ):
                        # Found the upstream node, let us remove it.
                        found_upstream_arg_nodes.append(arg)
                for arg_node in found_upstream_arg_nodes:
                    curr_node._bound_args = _remove_from_tuple_helper(
                        curr_node._bound_args, arg_node
                    )
                    upstream_seq_id = self.get_seq_id_by_uuid(
                        self._upstream_indexes[uuid][1]
                    )
                    downstream_seq_id = self.get_seq_id_by_uuid(uuid)
                    recv_op_node = remote(recv_op).bind(
                        self._party, upstream_seq_id, downstream_seq_id
                    )
                    curr_node._bound_args = _append_to_tuple_helper(
                        curr_node._bound_args, recv_op_node
                    )

            if uuid in self._downstream_indexes:
                # Add a `send_op` to the downstream. No need to remove
                # any downstream node.
                # TODO(qwang): 要记录下来send_op是一个final node, 他是需要触发执行的
                upstream_seq_id = self.get_seq_id_by_uuid(uuid)
                downstream_seq_id = self.get_seq_id_by_uuid(
                    self._downstream_indexes[uuid][1]
                )
                downstream_party = self._downstream_indexes[uuid][0]
                dest = self._cluster[downstream_party]
                send_op_node = remote(send_op).bind(
                    dest, curr_node, upstream_seq_id, downstream_seq_id
                )
                self._injected_send_ops.append(send_op_node)

    def _build_indexes(self) -> tuple:
        assert self._upstream_indexes is None, "shouldn't be called twice."
        assert self._downstream_indexes is None, "shouldn't be called twice."
        self._upstream_indexes = {}
        self._downstream_indexes = {}

        for uuid in self._all_node_info:
            _, this_node = self._all_node_info[uuid]
            party_of_this_node = this_node.get_party()

            all_dependencies = this_node._get_all_child_nodes()
            for dependency in all_dependencies:
                if not _is_fed_dag_node(dependency):
                    continue

                # Code path of fed node(one of FedClassNode, FedClassMethodNode and FedFunctionNode)
                party_of_dependency = dependency.get_party()
                if party_of_this_node == party_of_dependency:
                    # No need to do any thing for the case that this 2 nodes are in the
                    # same party.
                    continue
                # Code path for this 2 nodes are in different parties, so build the indexes.
                self._upstream_indexes[
                    this_node.get_stable_uuid()
                ] = (party_of_dependency, dependency.get_stable_uuid())
                self._downstream_indexes[
                    dependency.get_stable_uuid()
                ] = (party_of_this_node, this_node.get_stable_uuid())
        return self._upstream_indexes, self._downstream_indexes

    def _compute_nodes_need_to_drive(self):
        self._nodes_need_to_drive = self._nodes_need_to_drive + self._injected_send_ops
        if self._final_fed_dag_node.get_party() == self._party:
            self._nodes_need_to_drive.append(self._final_fed_dag_node)

    def get_nodes_to_drive(self):
        return self._nodes_need_to_drive

    def get_upstream_indexes(self):
        assert self._downstream_indexes is not None
        return self._upstream_indexes

    def get_downstream_indexes(self):
        assert self._downstream_indexes is not None
        return self._downstream_indexes

    def build(self):
        self._build()
        self._extract_local_dag()
        self._build_indexes()
        self._inject_barriers()
        self._compute_nodes_need_to_drive()

    def execute(self):
        object_refs = []
        for node in self._nodes_need_to_drive:
            object_refs.append(node.execute())
        print(f"Getting objects {object_refs}")
        print(ray.get(object_refs))


def build_fed_dag(fed_dag_node, party: str) -> FedDAG:
    assert fed_dag_node is not None
    fed_dag_handle = FedDAG(fed_dag_node, party=party)
    fed_dag_handle._build()
    fed_dag_handle._extract_local_dag()
    fed_dag_handle._build_indexes()
    fed_dag_handle._inject_barriers()
    fed_dag_handle._compute_nodes_need_to_drive()
    return fed_dag_handle


class FedRemoteFunction:
    def __init__(self, func_or_class) -> None:
        self._party = None
        self._func_body = func_or_class

    def party(self, party: str):
        self._party = party
        return self

    def bind(self, *args, **kwargs):
        return FedDAGFunctionNode(self._func_body, args, kwargs, self._party)


class FedRemoteClass:
    def __init__(self, func_or_class) -> None:
        self._party = None
        self._cls = func_or_class

    def party(self, party: str):
        self._party = party
        return self

    def bind(self, *args, **kwargs):
        return FedDAGClassNode(self._cls, self._party, args, kwargs)


# This is the decorator `@fed.remote`
def remote(*args, **kwargs):
    def _make_fed_remote(function_or_class, options=None):
        if inspect.isfunction(function_or_class) or is_cython(function_or_class):
            return FedRemoteFunction(function_or_class)

        if inspect.isclass(function_or_class):
            return FedRemoteClass(function_or_class)

        raise TypeError(
            "The @ray.remote decorator must be applied to either a function or a class."
        )

    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # This is the case where the decorator is just @fed.remote.
        return _make_fed_remote(args[0])
    assert len(args) == 0 and len(kwargs) > 0, "Remote args error."
    return functools.partial(_make_fed_remote, options=kwargs)
