
import pytest

import fed.tree_util as tree_utils

def test_flatten_none():
    # test None
    li, tree_def = tree_utils.flatten(None)    
    assert type(li) == list
    assert len(li) == 1
    res = tree_utils.unflatten(li, tree_def)
    assert res is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
