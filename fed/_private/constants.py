import os


RAYFED_CLUSTER_KEY = b"__RAYFED_CLUSTER"

RAYFED_PARTY_KEY = b"__RAYFED_PARTY"

RAYFED_TLS_CONFIG = b"__RAYFED_TLS_CONFIG"

RAYFED_LOG_FMT = "%(asctime)s %(levelname)s %(name)s [%(party)s] --  %(message)s"

RAYFED_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# Whitelist config file absolute path.
RAYFED_PICKLE_WHITELIST_CONFIG_PATH = os.environ.get(
    "RAYFED_PICKLE_WHITELIST_CONFIG_PATH", None
)
