
class GlobalContext:
    def __init__(self) -> None:
        self._seq_count = 0

    def next_seq_id(self):
        self._seq_count += 1
        return self._seq_count


_global_context = None


def get_global_context():
    global _global_context
    if _global_context is None:
        _global_context = GlobalContext()
    return _global_context
