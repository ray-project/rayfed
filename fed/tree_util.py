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

from typing import List, Any, Tuple, Dict

# [1, 2, (3, 4), {"a", "b"}]
# flattened: [1, 2, 3, 4, "b"]
#
#     1
# /   |   \
# 2   3   "b"
#     |
#     4


class PyTreeDef:
    def __init__(
            self, o: Any,
            childern: list,
            is_leaf: bool,
            the_type,
            dict_key=None,
    ) -> None:
        self._type = the_type
        self._is_leaf = is_leaf is not None and is_leaf
        self._childern = childern or ()
        self._dict_key = dict_key

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
        if self._is_leaf:
            return 1
        else:
            return sum(child.num_leaves for child in self._childern)


def _build_tree(o: Any, leaf_objs: List, dict_key=None):
    if isinstance(o, List):
        children = [_build_tree(child, leaf_objs) for child in o]
        return PyTreeDef(o, children, False, "list", dict_key)
    elif isinstance(o, Tuple):
        children = [_build_tree(child, leaf_objs) for child in o]
        return PyTreeDef(o, children, False, "tuple", dict_key)
    elif isinstance(o, Dict):
        children = [_build_tree(value, leaf_objs, key) for key, value in o.items()]
        return PyTreeDef(o, children, False, "dict", dict_key)
    else:
        # treat as leaf.
        leaf_objs.append(o)
        return PyTreeDef(o, None, True, "primitive", dict_key)


def flatten(o: Any):
    flattened_objs = []
    tree_def = _build_tree(o, flattened_objs)
    return flattened_objs, tree_def


def _build_object(tree_def: PyTreeDef, flattened_objs, result):
    if tree_def._type == "list":
        return [_build_object(
            child, flattened_objs, result) for child in tree_def._childern]
    if tree_def._type == "tuple":
        li = [_build_object(
            child, flattened_objs, result) for child in tree_def._childern]
        return tuple(li)
    if tree_def._type == "dict":
        d = {}
        for child in tree_def._childern:
            d[child._dict_key] = _build_object(child, flattened_objs, result)
        return d
    elif tree_def._type == "primitive":
        return flattened_objs.pop(0)
    else:
        raise KeyError("")


def unflatten(flattened_objs: List, tree_def: PyTreeDef):
    # we should clone flattened_objs ?
    result = _build_object(tree_def, flattened_objs, "")
    return result


print("flattening...")
o1 = [1, 2, (3, 4), [5, {"b", 6}, 7], 8]

li, t = flatten(o1)
print(t.num_leaves)
print(li)

print("unflattening...")
res = unflatten(li, t)
print(res)


# print("flattening...")
# o1 = [1, 2, [3, 4, [5, [6]]], 7, {8: 9}]
# li, t = tree_flatten(o1)
# print(t.num_leaves)
# print(li)

# li[0] = "hello_1"

# print("unflattening...")
# res = unflatten(t, li)
# print(res)
