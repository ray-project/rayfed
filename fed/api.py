import functools
from hashlib import new
import logging
from logging.config import listen

from typing import Any, Dict, List, Union, overload
from ray.dag.function_node import FunctionNode
from ray.dag.dag_node import DAGNode
from ray.dag.base import DAGNodeBase
import ray
from ray._private.inspect_util import is_cython
from ray.dag.class_node import ClassNode, _UnboundClassMethodNode, ClassMethodNode
from fed._private import api as _private_api
from fed import utils as fed_utils
from ray.dag.py_obj_scanner import _PyObjScanner
from ray.dag import PARENT_CLASS_NODE_KEY, PREV_CLASS_METHOD_CALL_KEY


logger = logging.getLogger(__file__)

from ray.dag import DAGNode as RayDAGNode

# GRPC related
import time
from concurrent import futures
import grpc
from fed import fed_pb2_grpc, fed_pb2
import inspect

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


def _is_fed_dag_node(node):
    # A helper that returns thether the argument is type of FedDAGNode.
    return (isinstance(node, FedDAGClassNode) or isinstance(node, FedDAGClassMethodNode)
     or isinstance(node, FedDAGFunctionNode))

class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, event, all_data):
        self._event = event
        self._all_data = all_data
    
    def SendData(self, request, context):
        upstream_seq_id = int(request.upstream_seq_id)
        downstream_seq_id = int(request.downstream_seq_id)
        print(f"Received a grpc data {request.data} from {upstream_seq_id} to {downstream_seq_id}")
        self._all_data[downstream_seq_id] = request.data
        self._event.set()
        print(f"=======Event set for {upstream_seq_id}")
        return fed_pb2.SendDataResponse(result="OK")

def _run_grpc_server(event, all_data):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(SendDataService(event, all_data), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("start service...")
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

def send_data_grpc(data, upstream_seq_id, downstream_seq_id):
    conn=grpc.insecure_channel('172.17.0.2:50052')
    client = fed_pb2_grpc.GrpcServiceStub(channel=conn)
    request = fed_pb2.SendDataRequest(data=data, upstream_seq_id=str(upstream_seq_id), downstream_seq_id=str(downstream_seq_id))
    response = client.SendData(request)
    print("received response:",response.result)
    return response.result

def remove_from_tuple_helper(tp, item):
    li = list(tp)
    li.remove(item)
    return tuple(li)

def append_to_tuple_helper(tp, item):
    li = list(tp)
    li.append(item)
    return tuple(li)


def send_op(data, upstream_seq_id, downstream_seq_id):
    # Not sure if here has a implicitly data fetching,
    # if yes, we should send data, otherwise we should
    # send `ray.get(data)`
    print(f"Sending data {data} to seq_id {downstream_seq_id} from {upstream_seq_id}")
    response = send_data_grpc(data, upstream_seq_id, downstream_seq_id)
    print(f"Sent. response is {response}")
    return True # True indicates it's sent successfully.


@ray.remote
class RecverProxyActor:
    def __init__(self, listening_addr: str):
        self._listening_addr = listening_addr

        # Workaround the threading coordinations

        # All events for grpc waitting usage.
        import threading
        self._event = threading.Event()
        self._event.set()     # 设定Flag = True
        self._all_data = {}

    def is_ready(self):
        return True

    def run_grpc_server(self):
        _run_grpc_server(self._event, self._all_data)

    def get_data(self, upstream_seq_id, curr_seq_id):
        print(f"Getting data for {curr_seq_id} from {upstream_seq_id}")
        self._event.clear()
        self._event.wait()
        print(f"=======Waited for {curr_seq_id}, data is {self._all_data[curr_seq_id]}")
        return self._all_data[curr_seq_id]


def recv_op(party_name, upstream_seq_id, curr_seq_id):
    recever_proxy = ray.get_actor(f"RecverProxyActor-{party_name}")
    data = recever_proxy.get_data.remote(upstream_seq_id, curr_seq_id)
    return ray.get(data)


class FedDAGFunctionNode(FunctionNode):
    def __init__(self, func_body, func_args, func_kwargs, party: str):
        self._func_body = func_body
        self._party = party
        super().__init__(func_body, func_args, func_kwargs, None)

    def get_func_or_method_name(self):
        return self._func_body.__name__

    def get_party(self):
        return self._party

class FedDAGClassNode(ClassNode):
    def __init__(self, cls, party, cls_args, cls_kwargs):
        self._party = party
        self._cls = cls
        super().__init__(cls, cls_args, cls_kwargs, {}) # TODO(qwang): Pass options instead of None.

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

class FedDAGClassMethodNode(ClassMethodNode):
    def __init__(self, party, method_name, method_args, method_kwargs, options, other_args_to_resolve=None):
        self._party = party
        self._method_name = method_name
        super().__init__(method_name, method_args, options, method_kwargs, other_args_to_resolve=other_args_to_resolve)

    def get_func_or_method_name(self):
        return self._method_name

    def get_party(self):
        return self._party
class FedDAG:
    def __init__(self, final_fed_dag_node, party_name: str=None):
        self._all_sub_dags = []
        self._final_fed_dag_node = final_fed_dag_node
        self._all_node_info = {}
        self._seq_count = 0
        self._local_dag_uuids = None
        self._party_name = party_name
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

        # Create RecevrProxyActor
        # Not that this is now a threaded actor.
        self._recver_proxy_actor = RecverProxyActor.options(name=f"RecverProxyActor-{self._party_name}", max_concurrency=1000).remote("xxxxx:yyy")
        self._recver_proxy_actor.run_grpc_server.remote()
        assert ray.get(self._recver_proxy_actor.is_ready.remote()) is True
        print("======== RecverProxy was created successfully.")

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
            if (isinstance(child_node, FedDAGFunctionNode) or isinstance(child_node, FedDAGClassNode)
                or isinstance(child_node, FedDAGClassMethodNode)):
                self.__generate_all_node_info(child_node)



    def _build(self):
        """ An internal API to build all the info of this dag.
        """
        self.__generate_all_node_info(self._final_fed_dag_node)

    def _extract_local_dag(self) -> None:
        assert self._local_dag_uuids is None, "This shouldn't be called twice."

        self._local_dag_uuids = []
        for uuid in self._all_node_info:
            _, node = self._all_node_info[uuid]
            options = node.get_options()
            assert options is not None
            if self._party_name == node.get_party():
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
                    if _is_fed_dag_node(arg) and arg.get_stable_uuid() == self._upstream_indexes[uuid]:
                        # Found the upstream node, let us remove it.
                        found_upstream_arg_nodes.append(arg)
                for arg_node in found_upstream_arg_nodes:
                    curr_node._bound_args = remove_from_tuple_helper(curr_node._bound_args, arg_node)
                    upstream_seq_id = self.get_seq_id_by_uuid(self._upstream_indexes[uuid])
                    downstream_seq_id = self.get_seq_id_by_uuid(uuid)
                    recv_op_node = remote(recv_op).bind(self._party_name, upstream_seq_id, downstream_seq_id)
                    curr_node._bound_args = append_to_tuple_helper(curr_node._bound_args, recv_op_node)

            if uuid in self._downstream_indexes:
                # Add a `send_op` to the downstream. No need to remove
                # any downstream node.
                # TODO(qwang): 要记录下来send_op是一个final node, 他是需要触发执行的
                upstream_seq_id = self.get_seq_id_by_uuid(uuid)
                downstream_seq_id = self.get_seq_id_by_uuid(self._downstream_indexes[uuid])
                send_op_node = remote(send_op).bind(curr_node, upstream_seq_id, downstream_seq_id)
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
                self._upstream_indexes[this_node.get_stable_uuid()] = dependency.get_stable_uuid()
                self._downstream_indexes[dependency.get_stable_uuid()] = this_node.get_stable_uuid()
        return self._upstream_indexes, self._downstream_indexes

    def _compute_nodes_need_to_drive(self):
        self._nodes_need_to_drive = self._nodes_need_to_drive + self._injected_send_ops
        if self._final_fed_dag_node.get_party() == self._party_name:
            self._nodes_need_to_drive.append(self._final_fed_dag_node)

    def get_nodes_to_drive(self):
        return self._nodes_need_to_drive

    def get_upstream_indexes(self):
        assert self._downstream_indexes is not None
        return self._upstream_indexes

    def get_downstream_indexes(self):
        assert self._downstream_indexes is not None
        return self._downstream_indexes

    def execute(self):
        object_refs = []
        for node in self._nodes_need_to_drive:
            object_refs.append(node.execute())
        print(f"Getting objects {object_refs}")
        print(ray.get(object_refs))


def build_fed_dag(fed_dag_node, party_name: str) -> FedDAG:
    assert fed_dag_node is not None
    fed_dag_handle = FedDAG(fed_dag_node, party_name)
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
        self._party_name = None
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
    assert (
        len(args) == 0 and len(kwargs) > 0
    ), "Remote args error."
    return functools.partial(_make_fed_remote, options=kwargs)
