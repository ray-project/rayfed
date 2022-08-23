from typing import (
    Optional,
    Union,
    List,
    Tuple,
    Dict,
    Any,
    TypeVar,
    Callable,
)
import uuid


# Get the party related resource.
def _get_party_resource(options: dict):
    for resource_key in options["resources"]:
        # TODO: prefix should be changed.
        if resource_key.startswith("RES_"):
            return resource_key
    assert False # This line shouldn't be reached.


def _split_in_party_name(all_dependencies: List, party_name: str):
    in_party = []
    cross_party = []
    print(f"=========={all_dependencies}")
    for dependency in all_dependencies:
        options = dependency.get_options()
        assert options is not None
        node_resource = _get_party_resource(options)
        if node_resource == f"RES_{party_name}":
            in_party.append(dependency)
        else: cross_party.append(dependency) 
    return in_party, cross_party

def _need_to_be_drived_in_this_party(node: "DAGNode", party_name: str):
    # TO BE FILLED
    pass

def _find_first_in_party_node(node: "DAGNode", party_name: str):
    # TO BE FILLED
    pass
