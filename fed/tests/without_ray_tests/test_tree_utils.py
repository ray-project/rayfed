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

from typing import Any, Dict, List, Tuple, Union

import pytest

import fed.tree_util as tree_utils


def test_flatten_none():
    li, tree_def = tree_utils.tree_flatten(None)
    assert isinstance(li, list)
    assert len(li) == 1
    res = tree_utils.tree_unflatten(li, tree_def)
    assert res is None


def test_flatten_single_primivite_elements():
    def _assert_flatten_single_element(target: Any):
        li, tree_def = tree_utils.tree_flatten(target)
        assert isinstance(li, list)
        assert len(li) == 1
        res = tree_utils.tree_unflatten(li, tree_def)
        assert res == target

    _assert_flatten_single_element(1)
    _assert_flatten_single_element(0.5)
    _assert_flatten_single_element("hello")
    _assert_flatten_single_element(b"world")


def test_flatten_single_simple_containers():
    def _assert_flatten_single_simple_container(target: Union[List, Tuple, Dict]):
        container_len = len(target)
        li, tree_def = tree_utils.tree_flatten(target)
        assert isinstance(li, list)
        assert len(li) == container_len
        res = tree_utils.tree_unflatten(li, tree_def)
        assert res == target

    _assert_flatten_single_simple_container([1, 2, 3])
    _assert_flatten_single_simple_container((1, 2, 3))
    _assert_flatten_single_simple_container({"a": 1, "b": 2, "c": 3})


def test_flatten_complext_nested_container():
    o = [1, 2, (3, 4), [5, {"b", 6}, 7], 8]
    flattened, tree_def = tree_utils.tree_flatten(o)
    assert len(flattened) == 8
    res = tree_utils.tree_unflatten(flattened, tree_def)
    assert o == res


def test_flatten_and_replace_element():
    o = [1, 2, (3, 4), [5, {"b": 6}, 7], 8]
    flattened, tree_def = tree_utils.tree_flatten(o)
    flattened[0] = "hello"
    flattened[5] = b"world"
    assert len(flattened) == 8
    res = tree_utils.tree_unflatten(flattened, tree_def)
    assert o != res
    assert len(res) == 5
    print(res)

    assert res[0] == "hello"
    assert res[1] == 2
    assert res[2] == (3, 4)
    assert res[3] == [5, {"b": b"world"}, 7]
    assert res[4] == 8


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
