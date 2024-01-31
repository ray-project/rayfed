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

import threading
from typing import Callable

from fed.cleanup import CleanupManager
from fed.exceptions import FedRemoteError


class GlobalContext:
    def __init__(
        self,
        job_name: str,
        current_party: str,
        sending_failure_handler: Callable[[Exception], None],
        exit_on_sending_failure=False,
        continue_waiting_for_data_sending_on_error=False,
    ) -> None:
        self._job_name = job_name
        self._seq_count = 0
        self._sending_failure_handler = sending_failure_handler
        self._exit_on_sending_failure = exit_on_sending_failure
        self._atomic_shutdown_flag_lock = threading.Lock()
        self._atomic_shutdown_flag = True
        self._cleanup_manager = CleanupManager(
            current_party, self.acquire_shutdown_flag
        )
        self._last_received_error: FedRemoteError = None
        self._continue_waiting_for_data_sending_on_error = (
            continue_waiting_for_data_sending_on_error
        )

    def next_seq_id(self) -> int:
        self._seq_count += 1
        return self._seq_count

    def get_cleanup_manager(self) -> CleanupManager:
        return self._cleanup_manager

    def get_job_name(self) -> str:
        return self._job_name

    def get_sending_failure_handler(self) -> Callable[[], None]:
        return self._sending_failure_handler

    def get_exit_on_sending_failure(self) -> bool:
        return self._exit_on_sending_failure

    def get_last_recevied_error(self) -> FedRemoteError:
        return self._last_received_error

    def set_last_recevied_error(self, err):
        self._last_received_error = err

    def get_continue_waiting_for_data_sending_on_error(self) -> bool:
        return self._continue_waiting_for_data_sending_on_error

    def acquire_shutdown_flag(self) -> bool:
        """
        Acquiring a lock and set the flag to make sure
        `fed.shutdown()` can be called only once.

        The unintended shutdown, i.e. `fed.shutdown(intended=False)`, needs to
        be executed only once. However, `fed.shutdown` may get called duing
        error handling, where acquiring lock inside `fed.shutdown` may cause
        dead lock, see `CleanupManager._signal_exit` for more details.

        Returns:
            bool: True if successfully get the permission to unintended shutdown.
        """
        with self._atomic_shutdown_flag_lock:
            if self._atomic_shutdown_flag:
                self._atomic_shutdown_flag = False
                return True
            return False


_global_context = None


def init_global_context(
    current_party: str,
    job_name: str,
    exit_on_sending_failure: bool,
    continue_waiting_for_data_sending_on_error: bool,
    sending_failure_handler: Callable[[Exception], None] = None,
) -> None:
    global _global_context
    if _global_context is None:
        _global_context = GlobalContext(
            job_name,
            current_party,
            exit_on_sending_failure=exit_on_sending_failure,
            continue_waiting_for_data_sending_on_error=continue_waiting_for_data_sending_on_error,
            sending_failure_handler=sending_failure_handler,
        )


def get_global_context():
    global _global_context
    return _global_context


def clear_global_context(wait_for_sending=False):
    global _global_context
    if _global_context is not None:
        _global_context.get_cleanup_manager().stop(wait_for_sending=wait_for_sending)
        _global_context = None
