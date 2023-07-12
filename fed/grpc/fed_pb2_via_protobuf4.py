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

# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: fed_4.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0b\x66\x65\x64_4.proto\"S\n\x0fSendDataRequest\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\x12\x17\n\x0fupstream_seq_id\x18\x02 \x01(\t\x12\x19\n\x11\x64ownstream_seq_id\x18\x03 \x01(\t\"\"\n\x10SendDataResponse\x12\x0e\n\x06result\x18\x01 \x01(\t2@\n\x0bGrpcService\x12\x31\n\x08SendData\x12\x10.SendDataRequest\x1a\x11.SendDataResponse\"\x00\x42\x03\x80\x01\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'fed_4_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\200\001\001'
  _globals['_SENDDATAREQUEST']._serialized_start=15
  _globals['_SENDDATAREQUEST']._serialized_end=98
  _globals['_SENDDATARESPONSE']._serialized_start=100
  _globals['_SENDDATARESPONSE']._serialized_end=134
  _globals['_GRPCSERVICE']._serialized_start=136
  _globals['_GRPCSERVICE']._serialized_end=200
# @@protoc_insertion_point(module_scope)
