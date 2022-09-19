import ray

from fed.grpc import fed_pb2_grpc
# GRPC related
import time
from concurrent import futures
import grpc
from fed.grpc import fed_pb2
import inspect

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

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


def send_op(data, upstream_seq_id, downstream_seq_id):
    # Not sure if here has a implicitly data fetching,
    # if yes, we should send data, otherwise we should
    # send `ray.get(data)`
    print(f"Sending data {data} to seq_id {downstream_seq_id} from {upstream_seq_id}")
    response = send_data_grpc(data, upstream_seq_id, downstream_seq_id)
    print(f"Sent. response is {response}")
    return True # True indicates it's sent successfully.


def recv_op(party_name, upstream_seq_id, curr_seq_id):
    recever_proxy = ray.get_actor(f"RecverProxyActor-{party_name}")
    data = recever_proxy.get_data.remote(upstream_seq_id, curr_seq_id)
    return ray.get(data)
