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


KEY_OF_CLUSTER_CONFIG = "CLUSTER_CONFIG"

KEY_OF_JOB_CONFIG = "JOB_CONFIG"

KEY_OF_GRPC_METADATA = "GRPC_METADATA"

KEY_OF_CLUSTER_ADDRESSES = "CLUSTER_ADDRESSES"

KEY_OF_CURRENT_PARTY_NAME = "CURRENT_PARTY_NAME"

KEY_OF_TLS_CONFIG = "TLS_CONFIG"

KEY_OF_CROSS_SILO_COMM_CONFIG_DICT = "CROSS_SILO_COMM_CONFIG_DICT"

RAYFED_LOG_FMT = "%(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)s [%(party)s] -- [%(jobname)s] %(message)s"  # noqa

RAYFED_DATE_FMT = "%Y-%m-%d %H:%M:%S"

RAY_VERSION_2_0_0_STR = "2.0.0"

RAYFED_DEFAULT_JOB_NAME = "Anonymous_job"

RAYFED_JOB_KV_DATA_KEY_FMT = "RAYFED#{}#{}"

RAYFED_DEFAULT_SENDER_PROXY_ACTOR_NAME = "SenderProxyActor"

RAYFED_DEFAULT_RECEIVER_PROXY_ACTOR_NAME = "ReceiverProxyActor"

RAYFED_DEFAULT_SENDER_RECEIVER_PROXY_ACTOR_NAME = "SenderReceiverProxyActor"
