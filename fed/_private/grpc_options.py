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
