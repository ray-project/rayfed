#!/usr/bin/env bash

set -e
set -x

# All tests should be ran with TLS enabled for Ray cluster.
python tool/generate_tls_certs.py
export RAY_USE_TLS=1
export RAY_TLS_SERVER_CERT="/tmp/rayfed/test-certs/server.crt"
export RAY_TLS_SERVER_KEY="/tmp/rayfed/test-certs/server.key"
export RAY_TLS_CA_CERT="/tmp/rayfed/test-certs/server.crt"

cd fed/tests
python3 -m pytest -v -s test_*
python3 -m pytest -v -s serializations_tests/test_*
python3 -m pytest -v -s multi-jobs/test_*
python3 -m pytest -v -s without_ray_tests/test_*
python3 -m pytest -v -s client_mode_tests/test_*
cd -

echo "All tests finished."
