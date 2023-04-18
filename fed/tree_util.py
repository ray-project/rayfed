from typing import Any, Callable, List, Tuple, Union
from dataclasses import dataclass

from typing import Any, Callable, List, Optional, Tuple
from typing import Any, Tuple, List
from typing import Any, Callable
from typing import Any, Callable, List, Optional, Tuple, Iterable


class PyTreeDef:
    def __init__(self, num_leaves: int, children: Tuple['PyTreeDef', ...] = None):
        self.num_leaves = num_leaves
        self.children = children or ()

    def __getitem__(self, index: int) -> 'PyTreeDef':
        return self.children[index]

    def __len__(self) -> int:
        return len(self.children)

    def __repr__(self) -> str:
        return f"PyTreeDef(num_leaves={self.num_leaves}, children={self.children})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PyTreeDef):
            return False
        return self.num_leaves == other.num_leaves and self.children == other.children


@dataclass
class PyTreeNode:
  value: Union["PyTreeDef", "Leaf"]
  children: List["PyTreeNode"]


Leaf = Any



def tree_flatten(tree: Any,
                 is_leaf: Optional[Callable[[Any], bool]] = None
                 ) -> Tuple[List[Leaf], PyTreeDef]:
  """Flattens a pytree.
  The flattening order (i.e. the order of elements in the output list)
  is deterministic, corresponding to a left-to-right depth-first tree
  traversal.
  Args:
    tree: a pytree to flatten.
    is_leaf: an optionally specified function that will be called at each
      flattening step. It should return a boolean, with true stopping the
      traversal and the whole subtree being treated as a leaf, and false
      indicating the flattening should traverse the current object.
  Returns:
    A pair where the first element is a list of leaf values and the second
    element is a treedef representing the structure of the flattened tree.
  """
  if is_leaf is None:
    is_leaf = lambda x: not isinstance(x, tuple)

  def build_node(x):
    if is_leaf(x):
      return PyTreeNode(x, [])
    else:
      children = [build_node(y) for y in x]
      value = PyTreeDef(len(children), [c.value for c in children])
      return PyTreeNode(value, children)

  root = build_node(tree)

  def dfs(node, leaves):
    if isinstance(node.value, PyTreeDef):
      for child in node.children:
        dfs(child, leaves)
    else:
      leaves.append(node.value)

  leaves = []
  dfs(root, leaves)
  return leaves, root.value


def unflatten(treedef: PyTreeDef, leaves: Iterable[Leaf]) -> Any:
  """Reconstructs a pytree from the treedef and the leaves.
  The inverse of :func:`tree_flatten`.
  Args:
    treedef: the treedef to reconstruct
    leaves: the iterable of leaves to use for reconstruction. The iterable
      must match the leaves of the treedef.
  Returns:
    The reconstructed pytree, containing the ``leaves`` placed in the structure
    described by ``treedef``.
  """
  def build_node(value):
    if isinstance(value, Leaf):
      return PyTreeNode(value, [])
    else:
      children = [build_node(v) for v in value.children]
      return PyTreeNode(children, value)

  root = build_node(treedef)
  iterator = iter(leaves)

  def dfs(node):
    if isinstance(node.value, PyTreeDef):
      for child in node.children:
        dfs(child)
    else:
      node.value = next(iterator)

  dfs(root)
  return root.value
