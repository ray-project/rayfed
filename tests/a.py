
def _tree_flatten(tree):
    if isinstance(tree, dict):
        items = sorted(tree.items())
        keys, children = zip(*items)
        for child in children:
            yield from _tree_flatten(child)
    else:
        for child in tree:
            if isinstance(child, (tuple, list, dict)):
                yield PyTreeDef(node=(_build_treedef(child, is_leaf=lambda x: isinstance(x, (tuple, list, dict))).node,))
                yield from _tree_flatten(child)
            else:
                yield child

def _build_treedef(tree, is_leaf):
    if is_leaf(tree):
        return PyTreeDef(leaf=type(tree))
    elif isinstance(tree, dict):
        items = sorted(tree.items())
        keys, children = zip(*items)
        subtrees = [_build_treedef(child, is_leaf) for child in children]
        return PyTreeDef(node=(keys, subtrees))
    else:
        children = [child for child in tree]
        subtrees = [_build_treedef(child, is_leaf) for child in children]
        return PyTreeDef(node=subtrees)


class PyTreeDef:
    """
    Defines the structure of a PyTree (a tree of Python containers).
    A PyTree is defined by a tree of node types (tuples, lists, or dicts) and leaf types (all other types).
    """
    def __init__(self, node=None, leaf=None):
        """
        Initializes a new PyTreeDef object.
        :param node: The tree of node types.
        :param leaf: The leaf type.
        """
        self.node = node or ()
        self.leaf = leaf or object()

    @property
    def num_nodes(self):
        """
        Gets the number of nodes in the PyTree.
        """
        return len(self.node)

    @property
    def num_leaves(self):
        """
        Gets the number of leaves in the PyTree.
        """
        return 1 if self.leaf is object() else 0

    @property
    def __str__(self) -> str:
        if self.leaf:
            return "*"
        else:
            return "#"

        
def tree_flatten(tree):
    leaves = []
    treedef = _build_treedef(tree, is_leaf=lambda x: not isinstance(x, (tuple, list)))
    for subtree in _tree_flatten(tree):
        if isinstance(subtree, tuple) and len(subtree) == 2 and isinstance(subtree[0], PyTreeDef):
            leaves.extend(subtree[1])
        else:
            leaves.append(subtree)
    return leaves, treedef


# 测试
tree = ((1, 2), 3, [4, 5, (6, 7)])
leaves, treedef = tree_flatten(tree)
print(leaves)
print(treedef.node)
print(treedef.leaf)

########################################################

def tree_unflatten(leaves, treedef):
    # 从treedef中解析出树的结构
    tree = _tree_unflatten_from_treedef(leaves, treedef)
    return tree

def _tree_unflatten_from_treedef(leaves, treedef):
    if treedef.num_leaves == 1:
        # 如果这是一个叶子节点，直接返回当前的第一个叶子节点
        return leaves[0], leaves[1:]
    else:
        # 否则构造一个新的子树
        subtrees = []
        for subtreedef in treedef.node:
            subtree, leaves = _tree_unflatten_from_treedef(leaves, subtreedef)
            subtrees.append(subtree)
        if isinstance(treedef.node, tuple):
            return tuple(subtrees), leaves
        elif isinstance(treedef.node, list):
            return subtrees, leaves
        elif isinstance(treedef.node, dict):
            return dict(zip(treedef.node[0], subtrees)), leaves

print("===============unflattening...")
new_tree = tree_unflatten(leaves, treedef)
print(new_tree)
