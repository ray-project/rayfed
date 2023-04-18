
import pytest

from typing import Any, Union, List, Tuple, Dict
import fed.tree_util as tree_utils

def test_flatten_none():
    li, tree_def = tree_utils.flatten(None)    
    assert type(li) == list
    assert len(li) == 1
    res = tree_utils.unflatten(li, tree_def)
    assert res is None

def test_flatten_single_primivite_elements():

    def _assert_flatten_single_element(target: Any):
        li, tree_def = tree_utils.flatten(target)    
        assert type(li) == list
        assert len(li) == 1
        res = tree_utils.unflatten(li, tree_def)
        assert res == target

    _assert_flatten_single_element(1)
    _assert_flatten_single_element(0.5)
    _assert_flatten_single_element("hello")
    _assert_flatten_single_element(b"world")

def test_flatten_single_simple_containers():
    def _assert_flatten_single_simple_container(target: Union[List, Tuple, Dict]):
        container_len = len(target)
        li, tree_def = tree_utils.flatten(target)    
        assert type(li) == list
        assert len(li) == container_len
        res = tree_utils.unflatten(li, tree_def)
        assert res == target

    _assert_flatten_single_simple_container([1, 2, 3])
    _assert_flatten_single_simple_container((1, 2, 3))
    _assert_flatten_single_simple_container({"a": 1, "b": 2, "c": 3})

def test_flatten_complext_nested_container():
    o = [1, 2, (3, 4), [5, {"b", 6}, 7], 8]
    flattened, tree_def = tree_utils.flatten(o)
    assert len(flattened) == 8
    res = tree_utils.unflatten(flattened, tree_def)
    assert o == res

def test_flatten_and_replace_element():
    o = [1, 2, (3, 4), [5, {"b": 6}, 7], 8]
    flattened, tree_def = tree_utils.flatten(o)
    flattened[0] = "hello"
    flattened[5] = b"world"
    assert len(flattened) == 8
    res = tree_utils.unflatten(flattened, tree_def)
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
