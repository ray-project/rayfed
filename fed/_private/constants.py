import json
import os


RAYFED_CLUSTER_KEY = b"__RAYFED_CLUSTER"

RAYFED_PARTY_KEY = b"__RAYFED_PARTY"

RAYFED_TLS_CONFIG = b"__RAYFED_TLS_CONFIG"

RAYFED_LOG_FMT = "%(asctime)s %(levelname)s %(name)s [%(party)s] --  %(message)s"

RAYFED_DATE_FMT = "%Y-%m-%d %H:%M:%S"


__GRPC_SERVICE_CONFIG_JSON_STRING = json.dumps({
    "methodConfig": [{
        "name": [{
            "service": "GrpcService"
        }],
        "retryPolicy": {
            "maxAttempts": 5,
            "initialBackoff": "5s",
            "maxBackoff": "30s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE"],
        },
    }]
})

GRPC_OPTIONS = [
    ('grpc.max_send_message_length', 100 * 1024 * 1024),
    ('grpc.max_receive_message_length', 100 * 1024 * 1024),
    ('grpc.enable_retries', 1),
    ('grpc.service_config', __GRPC_SERVICE_CONFIG_JSON_STRING),
    ]

# Whitelist config file absolute path.
RAYFED_PICKLE_WHITELIST_CONFIG_PATH = os.environ.get(
    "RAYFED_PICKLE_WHITELIST_CONFIG_PATH", None
)
