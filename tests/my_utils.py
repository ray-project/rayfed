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
    def __init__(self, o: Any, childern: list, is_leaf: bool) -> None:
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
        children_objs = o[1:]
        children = [_build_tree(o, leaf_objs) for o in children_objs]
        return PyTreeDef(o, children, False)
    elif isinstance(o, Tuple):
        raise KeyError("")
    elif isinstance(o, Dict):
        raise KeyError("")
    else:
        # treat as leaf.
        leaf_objs.append(o)
        return PyTreeDef(o, None, True)


def tree_flatten(o: Any):
    flattened_objs = []
    tree_def = _build_tree(o, flattened_objs)
    return flattened_objs, tree_def

o1 = [1, 2, [3, 4], 5]
li, t = tree_flatten(o1)
print(t.num_leaves)
print(li)

# for child in t._childern:
#     print(child._o)
