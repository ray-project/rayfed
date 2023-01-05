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

import json

_GRPC_RETRY_POLICY = {
    "maxAttempts": 5,
    "initialBackoff": "5s",
    "maxBackoff": "30s",
    "backoffMultiplier": 2,
    "retryableStatusCodes": ["UNAVAILABLE"],
}

_GRPC_SERVICE = "GrpcService"

_GRPC_MAX_SEND_MESSAGE_LENGTH = 500 * 1024 * 1024
_GRPC_MAX_RECEIVE_MESSAGE_LENGTH = 500 * 1024 * 1024


def get_grpc_options(
    retry_policy=None, max_send_message_length=None, max_receive_message_length=None
):
    if not retry_policy:
        retry_policy = _GRPC_RETRY_POLICY
    if not max_send_message_length:
        max_send_message_length = _GRPC_MAX_SEND_MESSAGE_LENGTH
    if not max_receive_message_length:
        max_receive_message_length = _GRPC_MAX_RECEIVE_MESSAGE_LENGTH

    return [
        (
            'grpc.max_send_message_length',
            max_send_message_length,
        ),
        (
            'grpc.max_receive_message_length',
            max_receive_message_length,
        ),
        ('grpc.enable_retries', 1),
        (
            'grpc.service_config',
            json.dumps(
                {
                    'methodConfig': [
                        {
                            'name': [{'service': _GRPC_SERVICE}],
                            'retryPolicy': retry_policy,
                        }
                    ]
                }
            ),
        ),
    ]
