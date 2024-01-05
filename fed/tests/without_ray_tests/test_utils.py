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

import pytest

import fed


@pytest.mark.parametrize(
    "input_address, is_valid_address",
    [
        ("192.168.0.1:8080", True),
        ("sa127032as:80", True),
        ("https://www.example.com", True),
        ("http://www.example.com", True),
        ("local", True),
        ("localhost", True),
        (None, False),
        ("invalid_string", False),
        ("http", False),
        ("example.com", False),
    ],
)
def test_validate_address(input_address, is_valid_address):
    if is_valid_address:
        fed.utils.validate_address(input_address)
    else:
        try:
            fed.utils.validate_address(input_address)
            assert False
        except Exception as e:
            assert isinstance(e, ValueError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
