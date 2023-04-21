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

# Most codes are copied from https://github.com/pytorch/pytorch/blob/c263bd43e8e8502d4726643bc6fd046f0130ac0e/torch/utils/_pytree.py # noqa

from typing import NamedTuple, Callable, Any, Tuple, List, Dict, Type, cast, TypeVar
from collections import namedtuple, OrderedDict
from dataclasses import dataclass


T = TypeVar('T')
S = TypeVar('S')
U = TypeVar('U')
R = TypeVar('R')

"""
Contains utility functions for working with nested python data structures.

A *pytree* is Python nested data structure. It is a tree in the sense that
nodes are Python collections (e.g., list, tuple, dict) and the leaves are
Python values. Furthermore, a pytree should not contain reference cycles.

pytrees are useful for working with nested collections of Tensors. For example,
one can use `tree_map` to map a function over all Tensors inside some nested
collection of Tensors and `tree_unflatten` to get a flat list of all Tensors
inside some nested collection. pytrees are helpful for implementing nested
collection support for PyTorch APIs.

This pytree implementation is not very performant due to Python overhead
To improve the performance we can move parts of the implementation to C++.
"""

# A NodeDef holds two callables:
# - flatten_fn should take the collection and return a flat list of values.
#   It can also return some context that is used in reconstructing the
#   collection.
# - unflatten_fn should take a flat list of values and some context
#   (returned by flatten_fn). It returns the collection by reconstructing
#   it from the list and the context.
Context = Any
PyTree = Any
FlattenFunc = Callable[[PyTree], Tuple[List, Context]]
UnflattenFunc = Callable[[List, Context], PyTree]


class NodeDef(NamedTuple):
    flatten_fn: FlattenFunc
    unflatten_fn: UnflattenFunc


SUPPORTED_NODES: Dict[Type[Any], NodeDef] = {}


def _register_pytree_node(
        typ: Any,
        flatten_fn: FlattenFunc,
        unflatten_fn: UnflattenFunc) -> None:
    SUPPORTED_NODES[typ] = NodeDef(flatten_fn, unflatten_fn)


def _dict_flatten(d: Dict[Any, Any]) -> Tuple[List[Any], Context]:
    return list(d.values()), list(d.keys())


def _dict_unflatten(values: List[Any], context: Context) -> Dict[Any, Any]:
    return dict(zip(context, values))


def _list_flatten(d: List[Any]) -> Tuple[List[Any], Context]:
    return d, None


def _list_unflatten(values: List[Any], context: Context) -> List[Any]:
    return list(values)


def _tuple_flatten(d: Tuple[Any, ...]) -> Tuple[List[Any], Context]:
    return list(d), None


def _tuple_unflatten(values: List[Any], context: Context) -> Tuple[Any, ...]:
    return tuple(values)


def _namedtuple_flatten(d: NamedTuple) -> Tuple[List[Any], Context]:
    return list(d), type(d)


def _namedtuple_unflatten(values: List[Any], context: Context) -> NamedTuple:
    return cast(NamedTuple, context(*values))


def _odict_flatten(d: 'OrderedDict[Any, Any]') -> Tuple[List[Any], Context]:
    return list(d.values()), list(d.keys())


def _odict_unflatten(values: List[Any], context: Context) -> 'OrderedDict[Any, Any]':
    return OrderedDict((key, value) for key, value in zip(context, values))


_register_pytree_node(dict, _dict_flatten, _dict_unflatten)
_register_pytree_node(list, _list_flatten, _list_unflatten)
_register_pytree_node(tuple, _tuple_flatten, _tuple_unflatten)
_register_pytree_node(namedtuple, _namedtuple_flatten, _namedtuple_unflatten)
_register_pytree_node(OrderedDict, _odict_flatten, _odict_unflatten)


# h/t https://stackoverflow.com/questions/2166818/how-to-check-if-an-object-is-an-instance-of-a-namedtuple # noqa
def _is_namedtuple_instance(pytree: Any) -> bool:
    typ = type(pytree)
    bases = typ.__bases__
    if len(bases) != 1 or bases[0] != tuple:
        return False
    fields = getattr(typ, '_fields', None)
    if not isinstance(fields, tuple):
        return False
    return all(type(entry) == str for entry in fields)


def _get_node_type(pytree: Any) -> Any:
    if _is_namedtuple_instance(pytree):
        return namedtuple
    return type(pytree)


# A leaf is defined as anything that is not a Node.
def _is_leaf(pytree: PyTree) -> bool:
    return _get_node_type(pytree) not in SUPPORTED_NODES


# A TreeSpec represents the structure of a pytree. It holds:
# "type": the type of root Node of the pytree
# context: some context that is useful in unflattening the pytree
# children_specs: specs for each child of the root Node
# num_leaves: the number of leaves
@dataclass
class TreeSpec:
    type: Any
    context: Context
    children_specs: List['TreeSpec']

    def __post_init__(self) -> None:
        self.num_leaves: int = sum([spec.num_leaves for spec in self.children_specs])

    def __repr__(self, indent: int = 0) -> str:
        repr_prefix: str = f'TreeSpec({self.type.__name__}, {self.context}, ['
        children_specs_str: str = ''
        if len(self.children_specs):
            indent += len(repr_prefix)
            children_specs_str += self.children_specs[0].__repr__(indent)
            children_specs_str += ',' if len(self.children_specs) > 1 else ''
            children_specs_str += ','.join(
                ['\n' + ' ' * indent + child.__repr__(indent)
                    for child in self.children_specs[1:]])
        repr_suffix: str = f'{children_specs_str}])'
        return repr_prefix + repr_suffix


class LeafSpec(TreeSpec):
    def __init__(self) -> None:
        super().__init__(None, None, [])
        self.num_leaves = 1

    def __repr__(self, indent: int = 0) -> str:
        return '*'


def tree_flatten(pytree: PyTree) -> Tuple[List[Any], TreeSpec]:
    """Flattens a pytree into a list of values and a TreeSpec that can be used
    to reconstruct the pytree.
    """
    if _is_leaf(pytree):
        return [pytree], LeafSpec()

    node_type = _get_node_type(pytree)
    flatten_fn = SUPPORTED_NODES[node_type].flatten_fn
    child_pytrees, context = flatten_fn(pytree)

    # Recursively flatten the children
    result : List[Any] = []
    children_specs : List['TreeSpec'] = []
    for child in child_pytrees:
        flat, child_spec = tree_flatten(child)
        result += flat
        children_specs.append(child_spec)

    return result, TreeSpec(node_type, context, children_specs)


def tree_unflatten(values: List[Any], spec: TreeSpec) -> PyTree:
    """Given a list of values and a TreeSpec, builds a pytree.
    This is the inverse operation of `tree_flatten`.
    """
    if not isinstance(spec, TreeSpec):
        raise ValueError(
            f'tree_unflatten(values, spec): Expected `spec` to be instance of '
            f'TreeSpec but got item of type {type(spec)}.')
    if len(values) != spec.num_leaves:
        raise ValueError(
            f'tree_unflatten(values, spec): `values` has length {len(values)} '
            f'but the spec refers to a pytree that holds {spec.num_leaves} '
            f'items ({spec}).')
    if isinstance(spec, LeafSpec):
        return values[0]

    unflatten_fn = SUPPORTED_NODES[spec.type].unflatten_fn

    # Recursively unflatten the children
    start = 0
    end = 0
    child_pytrees = []
    for child_spec in spec.children_specs:
        end += child_spec.num_leaves
        child_pytrees.append(tree_unflatten(values[start:end], child_spec))
        start = end

    return unflatten_fn(child_pytrees, spec.context)
