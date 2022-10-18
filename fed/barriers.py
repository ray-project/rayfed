import time

import cloudpickle
import grpc
import ray
import asyncio

from fed.grpc import fed_pb2, fed_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

def key_exists_in_two_dim_dict(the_dict, key_a, key_b) -> bool:
    if key_a not in the_dict:
        return False
    return key_b in the_dict[key_a]

def add_two_dim_dict(the_dict, key_a, key_b, val):
  if key_a in the_dict:
    the_dict[key_a].update({key_b: val})
  else:
    the_dict.update({key_a:{key_b: val}})

def get_from_two_dim_dict(the_dict, key_a, key_b):
    return the_dict[key_a][key_b]

class SendDataService(fed_pb2_grpc.GrpcServiceServicer):
    def __init__(self, all_events, all_data, party):
        self._events = all_events
        self._all_data = all_data
        self._party = party

    async def SendData(self, request, context):
        upstream_seq_id = int(request.upstream_seq_id)
        downstream_seq_id = int(request.downstream_seq_id)
        print(
            f"=====[{self._party}] Received a grpc data from {upstream_seq_id} to {downstream_seq_id}"
        )

        add_two_dim_dict(self._all_data, upstream_seq_id, downstream_seq_id, request.data)
        if not key_exists_in_two_dim_dict(self._events, upstream_seq_id, downstream_seq_id):
            event = asyncio.Event()
            add_two_dim_dict(self._events, upstream_seq_id, downstream_seq_id, event)
        event = get_from_two_dim_dict(self._events, upstream_seq_id, downstream_seq_id)
        event.set()
        print(f"=======[{self._party}] Event set for {upstream_seq_id}")
        return fed_pb2.SendDataResponse(result="OK")


async def _run_grpc_server(port, event, all_data, party):
    server = grpc.aio.server()
    fed_pb2_grpc.add_GrpcServiceServicer_to_server(SendDataService(event, all_data, party), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start()
    print("start service...")
    await server.wait_for_termination()


async def send_data_grpc(dest, data, upstream_seq_id, downstream_seq_id):
    data = cloudpickle.dumps(data)
    async with grpc.aio.insecure_channel(dest) as channel:
        stub = fed_pb2_grpc.GrpcServiceStub(channel)
        request = fed_pb2.SendDataRequest(
            data=data,
            upstream_seq_id=str(upstream_seq_id),
            downstream_seq_id=str(downstream_seq_id),
        )
        response = await stub.SendData(request)
        print("received response:", response.result)
        return response.result


def send_op(party, dest, data, upstream_seq_id, downstream_seq_id):
    # Not sure if here has a implicitly data fetching,
    # if yes, we should send data, otherwise we should
    # send `ray.get(data)`
    print(f"====[{party}] Sending data to seq_id {downstream_seq_id} from {upstream_seq_id}")
    response = asyncio.get_event_loop().run_until_complete(send_data_grpc(dest, data, upstream_seq_id, downstream_seq_id))
    print(f"Sent. response is {response}")
    return True  # True indicates it's sent successfully.


@ray.remote
class RecverProxyActor:
    async def __init__(self, listen_addr: str, party: str):
        self._listen_addr = listen_addr
        self._party = party

        # Workaround the threading coordinations

        # All events for grpc waitting usage.
        self._events = {} # map from (upstream_seq_id, downstream_seq_id) to event
        self._all_data = {} # map from (upstream_seq_id, downstream_seq_id) to data 

    async def run_grpc_server(self):
        return await _run_grpc_server(
            self._listen_addr[self._listen_addr.index(':') + 1 :],
            self._events,
            self._all_data,
            self._party,
        )

    async def is_ready(self):
        return True

    async def get_data(self, upstream_seq_id, curr_seq_id):
        print(f"====[{self._party}] Getting data for {curr_seq_id} from {upstream_seq_id}")
        if not key_exists_in_two_dim_dict(self._events, upstream_seq_id, curr_seq_id):
            add_two_dim_dict(self._events, upstream_seq_id, curr_seq_id, asyncio.Event())
        curr_event = get_from_two_dim_dict(self._events, upstream_seq_id, curr_seq_id)
        await curr_event.wait()
        print(f"=======[{self._party}] Waited for {curr_seq_id}")
        data = get_from_two_dim_dict(self._all_data, upstream_seq_id, curr_seq_id)
        return cloudpickle.loads(data)


def recv_op(party: str, upstream_seq_id, curr_seq_id):
    assert party
    receiver_proxy = ray.get_actor(f"RecverProxyActor-{party}")
    data = receiver_proxy.get_data.remote(upstream_seq_id, curr_seq_id)
    return ray.get(data)


def start_recv_proxy(listen_addr, party):
    # Create RecevrProxyActor
    # Not that this is now a threaded actor.
    recver_proxy_actor = RecverProxyActor.options(
        name=f"RecverProxyActor-{party}", max_concurrency=1000
    ).remote(listen_addr, party)
    recver_proxy_actor.run_grpc_server.remote()
    assert ray.get(recver_proxy_actor.is_ready.remote())
    print("======== RecverProxy was created successfully.")
