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
from typing import Callable


class GlobalContext:
    def __init__(self, job_name: str,
                 failure_handler: Callable[[], None]) -> None:
        self._job_name = job_name
        self._seq_count = 0
        self._cleanup_manager = CleanupManager()
        self._failure_handler = failure_handler

    def next_seq_id(self) -> int:
        self._seq_count += 1
        return self._seq_count

    def get_cleanup_manager(self) -> CleanupManager:
        return self._cleanup_manager

    def job_name(self) -> str:
        return self._job_name

    def failure_handler(self) -> Callable[[], None]:
        return self._failure_handler


_global_context = None


def init_global_context(job_name: str, failure_handler: Callable[[], None]) -> None:
    global _global_context
    if _global_context is None:
        _global_context = GlobalContext(job_name, failure_handler)


def get_global_context():
    global _global_context
    # if _global_context is None:
    #     _global_context = GlobalContext()
    return _global_context


def clear_global_context():
    global _global_context
    if _global_context is not None:
        _global_context.get_cleanup_manager().graceful_stop()
        _global_context = None
