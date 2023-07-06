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

from fed.cleanup import CleanupManager

class GlobalContext:
    def __init__(self) -> None:
        self._seq_count = 0
        self._cleanup_manager = CleanupManager()

    def set_exit_on_failure_sending(self, func):
        self._cleanup_manager.set_exit_on_failure_sending(func)

    def next_seq_id(self):
        self._seq_count += 1
        return self._seq_count


_global_context = None


def get_global_context():
    global _global_context
    if _global_context is None:
        _global_context = GlobalContext()
    return _global_context


def clear_global_context():
    global _global_context
    _global_context = None
