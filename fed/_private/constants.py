# Copyright 2022 Ant Group Co., Ltd.
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


RAYFED_CLUSTER_KEY = b"__RAYFED_CLUSTER"

RAYFED_PARTY_KEY = b"__RAYFED_PARTY"

RAYFED_TLS_CONFIG = b"__RAYFED_TLS_CONFIG"

RAYFED_CROSS_SILO_SERIALIZING_ALLOWED_LIST = b"__RAYFED_CROSS_SILO_SERIALIZING_"
    "ALLOWED_LIST"

RAYFED_LOG_FMT = "%(asctime)s %(levelname)s %(filename)s:%(lineno)s [%(party)s] "
    "--  %(message)s"

RAYFED_DATE_FMT = "%Y-%m-%d %H:%M:%S"
