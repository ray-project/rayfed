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


KEY_OF_CLUSTER_CONFIG = b"CLUSTER_CONFIG"

KEY_OF_JOB_CONFIG = b"JOB_CONFIG"

KEY_OF_GRPC_METADATA = b"GRPC_METADATA"

KEY_OF_CLUSTER_ADDRESSES = "CLUSTER_ADDRESSES"

KEY_OF_CURRENT_PARTY_NAME = "CURRENT_PARTY_NAME"

KEY_OF_TLS_CONFIG = "TLS_CONFIG"

KEY_OF_CROSS_SILO_MESSAGE_CONFIG = "CROSS_SILO_MESSAGE_CONFIG"

RAYFED_LOG_FMT = "%(asctime)s %(levelname)s %(filename)s:%(lineno)s [%(party)s] --  %(message)s" # noqa

RAYFED_DATE_FMT = "%Y-%m-%d %H:%M:%S"

RAY_VERSION_2_0_0_STR = "2.0.0"
