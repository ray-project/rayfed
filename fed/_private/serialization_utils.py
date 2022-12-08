import yaml
import io
import cloudpickle
import fed


_pickle_whitelist = None


def _restricted_loads(
    serialized_data,
    *,
    fix_imports=True,
    encoding="ASCII",
    errors="strict",
    buffers=None,
):
    import pickle5

    class RestrictedUnpickler(pickle5.Unpickler):
        def find_class(self, module, name):
            if _pickle_whitelist is None or (
                module in _pickle_whitelist
                and (_pickle_whitelist[module] is None or name in _pickle_whitelist[module])
            ):
                return super().find_class(module, name)

            if module == "fed._private": # TODO(qwang): Not sure if it works.
                return super().find_class(module, name)

            # Forbid everything else.
            raise pickle5.UnpicklingError("global '%s.%s' is forbidden" % (module, name))

    if isinstance(serialized_data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(serialized_data)
    return RestrictedUnpickler(
        file, fix_imports=fix_imports, buffers=buffers, encoding=encoding, errors=errors
    ).load()


def _apply_loads_function_with_whitelist():
    global _pickle_whitelist
    whitelist_path = fed._private.constants.RAYFED_PICKLE_WHITELIST_CONFIG_PATH
    if whitelist_path is None:
        return

    _pickle_whitelist = yaml.safe_load(open(whitelist_path, "rt")).get(
        "pickle_whitelist", None
    )
    if _pickle_whitelist is None:
        return

    if "*" in _pickle_whitelist:
        _pickle_whitelist = None
    for module, attr_list in _pickle_whitelist.items():
        if "*" in attr_list:
            _pickle_whitelist[module] = None
    cloudpickle.loads = _restricted_loads
