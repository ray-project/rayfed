from hashlib import new
import logging
from logging.config import listen
from typing import Any, Dict, List, Union, overload
from ray.dag.function_node import FunctionNode
from ray.dag.dag_node import DAGNode
from ray.dag.base import DAGNodeBase
import ray

from fed._private import api as _private_api
from fed import utils as fed_utils
from ray.dag.py_obj_scanner import _PyObjScanner
logger = logging.getLogger(__file__)

from ray.dag import DAGNode as RayDAGNode

# GRPC related
import time
from concurrent import futures
import grpc
from fed import fed_pb2_grpc, fed_pb2

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self):
        pass
    
    def SendData(self, request, context):
        print(f"Received a grpc data {request.data} from {request.upstream_seq_id} to {request.downstream_seq_id}")
        return fed_pb2.SendDataResponse(result="OK")

def _run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(SendDataService(), server)
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
    request = fed_pb2.SendDataRequest(data=data, upstream_seq_id=upstream_seq_id, downstream_seq_id=downstream_seq_id)
    response = client.SendData(request)
    print("received:",response.result)
    return response.result


def is_node_in_party(node: DAGNode, party_name: str):
    assert node is not None
    assert party_name is not None
    return f"RES_{party_name}" in node.get_options()["resources"]

def remove_from_tuple_helper(tp, item):
    li = list(tp)
    li.remove(item)
    return tuple(li)

def append_to_tuple_helper(tp, item):
    li = list(tp)
    li.append(item)
    return tuple(li)

@ray.remote
def send_op(data, upstream_seq_id, downstream_seq_id):
    # Not sure if here has a implicitly data fetching,
    # if yes, we should send data, otherwise we should
    # send `ray.get(data)`
    print(f"Sending data{data} to seq_id {downstream_seq_id} from {upstream_seq_id}")
    # response = send_data_grpc(data, upstream_seq_id, downstream_seq_id)
    # print(f"Sent. response is {response}")
    return True # True indicates it's sent successfully.


@ray.remote
class RecverProxyActor:
    def __init__(self, listening_addr: str):
        self._listening_addr = listening_addr

    async def run_grpc_server(self):
        _run_grpc_server()

    def get_data(self, upstream_seq_id, curr_seq_id):
        print(f"Getting data for {curr_seq_id} from {upstream_seq_id}")
        return "xxxx"


class FedDAGNode(RayDAGNode):
    def __init__(self, ray_dag_node: RayDAGNode):
        assert isinstance(ray_dag_node, RayDAGNode)
        self._ray_dag_node: RayDAGNode = ray_dag_node
        super().__init__(
            ray_dag_node._bound_args,
            ray_dag_node._bound_kwargs,
            ray_dag_node._bound_options,
            ray_dag_node._bound_other_args_to_resolve)

    def _get_uuid(self) -> str:
        return self._ray_dag_node.get_stable_uuid()

    def __str__(self) -> str:
        return self._ray_dag_node.__str__()

    def get_options(self) -> dict:
        return self._ray_dag_node.get_options()

    def get_other_args_to_resolve(self):
        return self._ray_dag_node.get_other_args_to_resolve()

    def get_kwargs(self):
        return self._ray_dag_node.get_kwargs()

    # def get_args(self):
    #     return self._ray_dag_node.get_args()

    def get_func_name(self):
        # TODO(qwang): Refine to `get_func_name()`
        if isinstance(self._ray_dag_node, FunctionNode):
            return self._ray_dag_node._body.__name__
        return self._ray_dag_node._method_name
    
    def _execute_impl(self, *args, **kwargs) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this node, assuming args have been transformed already."""
        # print(f"=======Submitting {self.get_func_name()}")
        return self._ray_dag_node._execute_impl(args, kwargs)

    def execute(self, *args, **kwargs) -> Union[ray.ObjectRef, ray.actor.ActorHandle]:
        """Execute this DAG using the Ray default executor."""
        return self.apply_recursive(lambda node: node._execute_impl(*args, **kwargs))

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ) -> "DAGNode":
        return FedDAGNode(self._ray_dag_node._copy(
            new_args=new_args,
            new_kwargs=new_kwargs,
            new_options=new_options,
            new_other_args_to_resolve=new_other_args_to_resolve))

class FedDAG(RayDAGNode):
    def __init__(self, final_ray_node: RayDAGNode, party_name: str=None):
        self._all_sub_dags = []
        self._final_ray_node = final_ray_node
        self._final_fed_node = None
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

    def get_all_node_info(self):
        return self._all_node_info

    def get_curr_seq_count(self):
        return self._seq_count

    def get_local_dag_uuids(self):
        assert self._local_dag_uuids is not None, "Local DAG should be extracted first."
        return self._local_dag_uuids

    def __generate_all_node_info(self)-> FedDAGNode:
        """This help method is used to generate the indexes from
        node uuid to the node entry and node seq id.
        """
        def __fill_index_dict(node: RayDAGNode):
            assert isinstance(node, RayDAGNode)
            fed_node = FedDAGNode(node)
            self._all_node_info[node.get_stable_uuid()] = (self._seq_count, fed_node)
            self._seq_count += 1
            return fed_node

        final_fed_node: FedDAGNode = self._final_ray_node.apply_recursive(__fill_index_dict)
        return final_fed_node

    def _build(self):
        """ An internal API to build all the info of this dag.
        """
        self._final_fed_node = self.__generate_all_node_info()
        self._final_ray_node = None # We should use _final_fed_node._ray_dag_node instead

    def _extract_local_dag(self) -> None:
        assert self._local_dag_uuids is None, "This shouldn't be called twice."

        self._local_dag_uuids = []
        for uuid in self._all_node_info:
           _, node = self._all_node_info[uuid]
           options = node.get_options()
           assert options is not None
           resources = options["resources"]
           if f"RES_{self._party_name}" in resources:
            self._local_dag_uuids.append(uuid)

    def _inject_barriers(self) -> None:
        # Create a RecverProxy for this party.
        assert self._recver_proxy_actor_node is None, "Barriers had been injected."
        self._recver_proxy_actor_node = RecverProxyActor.bind(listening_addr="127.0.0.1:11122")
        run_grpc_node = self._recver_proxy_actor_node.run_grpc_server.bind()
        # This increse 2 Proxy process there. We should fix it by use a remote actor or use the
        # same node.
        run_grpc_node.execute()

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
                    if isinstance(arg, FedDAGNode) and arg._get_uuid() == self._upstream_indexes[uuid]:
                        # Found the upstream node, let us remove it.
                        found_upstream_arg_nodes.append(arg)
                for arg_node in found_upstream_arg_nodes:
                    curr_node._bound_args = remove_from_tuple_helper(curr_node._bound_args, arg_node)
                    recv_op = self._recver_proxy_actor_node.get_data.bind(self._upstream_indexes[uuid], uuid)
                    curr_node._bound_args = append_to_tuple_helper(curr_node._bound_args, FedDAGNode(recv_op))

            if uuid in self._downstream_indexes:
                # Add a `send_op` to the downstream. No need to remove
                # any downstream node.
                # TODO(qwang): 要记录下来send_op是一个final node, 他是需要触发执行的
                send_op_node = send_op.bind(curr_node._ray_dag_node, curr_node._get_uuid(), uuid)
                self._injected_send_ops.append(FedDAGNode(send_op_node))


    def _build_indexes(self) -> tuple:
        assert self._upstream_indexes is None, "shouldn't be called twice."
        assert self._downstream_indexes is None, "shouldn't be called twice."
        self._upstream_indexes = {}
        self._downstream_indexes = {}

        for uuid in self._all_node_info:
            _, this_node = self._all_node_info[uuid]
            this_node_party_res = fed_utils._get_party_resource(this_node.get_options())
            scanner = _PyObjScanner(FedDAGNode)

            for dependency in scanner.find_nodes(
                [
                    this_node._bound_args,
                    this_node._ray_dag_node._bound_kwargs,
                    this_node._ray_dag_node._bound_other_args_to_resolve,
                ]
            ):
                dependency_party_res = fed_utils._get_party_resource(dependency.get_options())
                if this_node_party_res == dependency_party_res:
                    # No need to do any thing for the case that this 2 nodes are in the
                    # same party.
                    continue
                # Code path for this 2 nodes are in different parties, so build the indexes.
                self._upstream_indexes[this_node._get_uuid()] = dependency._get_uuid()
                self._downstream_indexes[dependency._get_uuid()] = this_node._get_uuid()
        return self._upstream_indexes, self._downstream_indexes

    def _compute_nodes_need_to_drive(self):
        self._nodes_need_to_drive = self._nodes_need_to_drive + self._injected_send_ops
        if is_node_in_party(self._final_fed_node, self._party_name):
            self._nodes_need_to_drive.append(self._final_fed_node)

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


def build_fed_dag(ray_dag: RayDAGNode, party_name: str) -> FedDAG:
    assert ray_dag is not None
    fed_dag_handle = FedDAG(ray_dag, party_name)
    fed_dag_handle._build()
    fed_dag_handle._extract_local_dag()
    fed_dag_handle._build_indexes()
    fed_dag_handle._inject_barriers()
    fed_dag_handle._compute_nodes_need_to_drive()
    return fed_dag_handle
