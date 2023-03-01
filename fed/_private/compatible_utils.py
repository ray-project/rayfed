import ray
import fed._private.constants as fed_constants


def _compare_ray_version_strings(version1, version2):
    """
    This utility function compares two Ray version strings and
    returns True if version1 is greater or they are equal,
    and False if version2 is greater.
    """
    v1_list = version1.split('.')
    v2_list = version2.split('.')
    len1 = len(v1_list)
    len2 = len(v2_list)

    for i in range(min(len1, len2)):
        if v1_list[i] == v2_list[i]:
            continue
        else:
            break

    return int(v1_list[i]) > int(v2_list[i])


def _ray_version_greater_than_2_0_0():
    return _compare_ray_version_strings(
        ray.__version__, fed_constants.RAY_VERSION_2_0_0_STR)


def init_ray(address: str=None, **kwargs):
    if address == "local" and _ray_version_greater_than_2_0_0():
        ray.init(**kwargs)
    else:
        ray.init(address=address, **kwargs)

def get_gcs_address_from_ray_worker():
    try:
        return ray._private.worker._global_node.gcs_address
    except AttributeError:
        return ray.worker._global_node.gcs_address
