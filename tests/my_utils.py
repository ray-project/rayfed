from typing import List, Any, Tuple, Dict

# [1, 2, (3, 4), {"a", "b"}]
# flattened: [1, 2, 3, 4, "b"]
#
#     1
# /   |   \
# 2   3   "b"
#     |
#     4


class PyTreeDef: # should be renamed to PyTreeNode
    def __init__(self, o: Any, childern: list, is_leaf: bool, the_type) -> None:
        self._type = the_type
        self._is_leaf = is_leaf is not None and is_leaf
        self._childern = childern or ()
        self._o = o

    @property
    def num_nodes(self):
        """
        Gets the number of nodes in the PyTree.
        """
        return len(self._childern)

    @property
    def num_leaves(self):
        """
        Gets the number of leaves in the PyTree.
        """
        num = 0
        if self._is_leaf:
            return 1
        else:
            return sum(child.num_leaves for child in self._childern)
    

def _build_tree(o: Any, leaf_objs: List):
    if isinstance(o, List):
        children = [_build_tree(child, leaf_objs) for child in o]
        return PyTreeDef(o, children, False, "list")
    elif isinstance(o, Tuple):
        raise KeyError("")
    elif isinstance(o, Dict):
        raise KeyError("")
    else:
        # treat as leaf.
        leaf_objs.append(o)
        return PyTreeDef(o, None, True, "primitive")


def tree_flatten(o: Any):
    flattened_objs = []
    tree_def = _build_tree(o, flattened_objs)
    return flattened_objs, tree_def


def _build_object(tree_def: PyTreeDef, flattened_objs, result):
    if tree_def._type == "list":
        return [_build_object(child, flattened_objs, result) for child in tree_def._childern]
    elif tree_def._type == "primitive":
        return tree_def._o
    else:
        raise KeyError("")

def tree_unflatten(flattened_objs: List, tree_def: PyTreeDef):
    # we should clone flattened_objs ?
    result = _build_object(tree_def, flattened_objs, "")
    return result

print("flattening...")
o1 = [1, 2, [3, 4, [5, [6]]], 7]
li, t = tree_flatten(o1)
print(t.num_leaves)
print(li)

print("unflattening...")
res = tree_unflatten(li, t)
print(res)
