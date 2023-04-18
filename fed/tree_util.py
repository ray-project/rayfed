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


class PyTreeDef:
    """ The tree node defination for representing for a complex nested python
        container object.

        for instance, the python object `[1, 2, (3, 4), {"a", "b"}]` should be
        represented as the tree:
                                   list
                                /  /  \  \
                               /  /    \   \
                              1   2  tuple dict
                                      / \    |
                                     3   4  "b"

        We now only support `List`, `Dict` and `Tuple`, as the container type.
    """
    def __init__(
            self,
            childern: list,
            is_leaf: bool,
            the_type,
            dict_key=None,
    ) -> None:
        # The type of this tree node, indicating a container node or a
        # primitive node. The leaf node is a primitive node.
        self._type = the_type
        # Whether this is a leaf node.
        self._is_leaf = is_leaf is not None and is_leaf
        # The children node(s) of this node.
        self._childern = childern or ()
        # If the parent is a `dict` node, this is a key of one of the dict items.
        # Note that this is enabled only if the parent node is a `dict` node.
        self._dict_key = dict_key


def _build_tree(o: Any, leaf_objs: List, dict_key=None):
    """ An internal function to build a tree according to the given object `o`.

        `leaf_objs` is an output parameter, that's used for collecting
        the leaves when building.

        `dict_key` is used for a dict item only.
    """
    if isinstance(o, List):
        children = [_build_tree(child, leaf_objs) for child in o]
        return PyTreeDef(children, False, "list", dict_key)
    elif isinstance(o, Tuple):
        children = [_build_tree(child, leaf_objs) for child in o]
        return PyTreeDef(children, False, "tuple", dict_key)
    elif isinstance(o, Dict):
        children = [_build_tree(value, leaf_objs, key) for key, value in o.items()]
        return PyTreeDef(children, False, "dict", dict_key)
    else:
        # Treat as a leaf.
        leaf_objs.append(o)
        return PyTreeDef(None, True, "primitive", dict_key)


def _build_object(tree_def: PyTreeDef, flattened_objs):
    """ Build the object from the `flattened_object` according to
        the given `tree_def`.
    """
    if tree_def._type == "list":
        return [_build_object(
            child, flattened_objs) for child in tree_def._childern]
    if tree_def._type == "tuple":
        li = [_build_object(
            child, flattened_objs) for child in tree_def._childern]
        return tuple(li)
    if tree_def._type == "dict":
        d = {}
        for child in tree_def._childern:
            d[child._dict_key] = _build_object(child, flattened_objs)
        return d
    elif tree_def._type == "primitive":
        return flattened_objs.pop(0)
    else:
        raise TypeError(f"Unsupported type {tree_def._type}.")


def flatten(o: Any):
    """Flattens an python object.

    The flattening order (i.e. the order of elements in the output list)
    is deterministic, corresponding to a left-to-right depth-first tree
    traversal.

    Args:
      o: The python object to flatten.
    Returns:
      A pair where the first element is a list of leaf values and the second
      element is a treedef representing the structure of the flattened tree.
    """
    flattened_objs = []
    tree_def = _build_tree(o, flattened_objs)
    return flattened_objs, tree_def


def unflatten(flattened_objs: List, tree_def: PyTreeDef):
    """Reconstructs a python object from the treedef and the leaves.

    The inverse of :func:`tree_flatten`.

    Args:
        leaves: the iterable of leaves to use for reconstruction. The iterable
            must match the leaves of the treedef.
        treedef: the treedef to reconstruct.

    Returns:
      The reconstructed python object, containing the `leaves` placed in the structure
      described by `treedef`.
    """
    return _build_object(tree_def, flattened_objs)
