# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: fed.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tfed.proto\"v\n\x0fSendDataRequest\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\x17\n\x0fupstream_seq_id\x18\x02 \x01(\t\x12\x19\n\x11\x64ownstream_seq_id\x18\x03 \x01(\t\x12!\n\x19serialized_invoking_frame\x18\x04 \x01(\x0c\"\"\n\x10SendDataResponse\x12\x0e\n\x06result\x18\x01 \x01(\t2@\n\x0bGrpcService\x12\x31\n\x08SendData\x12\x10.SendDataRequest\x1a\x11.SendDataResponse\"\x00\x42\x03\x80\x01\x01\x62\x06proto3')



_SENDDATAREQUEST = DESCRIPTOR.message_types_by_name['SendDataRequest']
_SENDDATARESPONSE = DESCRIPTOR.message_types_by_name['SendDataResponse']
SendDataRequest = _reflection.GeneratedProtocolMessageType('SendDataRequest', (_message.Message,), {
  'DESCRIPTOR' : _SENDDATAREQUEST,
  '__module__' : 'fed_pb2'
  # @@protoc_insertion_point(class_scope:SendDataRequest)
  })
_sym_db.RegisterMessage(SendDataRequest)

SendDataResponse = _reflection.GeneratedProtocolMessageType('SendDataResponse', (_message.Message,), {
  'DESCRIPTOR' : _SENDDATARESPONSE,
  '__module__' : 'fed_pb2'
  # @@protoc_insertion_point(class_scope:SendDataResponse)
  })
_sym_db.RegisterMessage(SendDataResponse)

_GRPCSERVICE = DESCRIPTOR.services_by_name['GrpcService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\200\001\001'
  _SENDDATAREQUEST._serialized_start=13
  _SENDDATAREQUEST._serialized_end=131
  _SENDDATARESPONSE._serialized_start=133
  _SENDDATARESPONSE._serialized_end=167
  _GRPCSERVICE._serialized_start=169
  _GRPCSERVICE._serialized_end=233
# @@protoc_insertion_point(module_scope)
