#!/usr/bin/env bash

set -e
set -x

# All tests should be ran with TLS enabled for Ray cluster.
python tool/generate_tls_certs.py
export RAY_USE_TLS=1
export RAY_TLS_SERVER_CERT="/tmp/rayfed/test-certs/server.crt"
export RAY_TLS_SERVER_KEY="/tmp/rayfed/test-certs/server.key"
export RAY_TLS_CA_CERT="/tmp/rayfed/test-certs/server.crt"

directory="fed/tests"
command="pytest -vs"
 
find "$directory" -type f -mindepth 1 -name "test_*" -exec bash -c "eval '$command'" \;

echo "All tests finished."
