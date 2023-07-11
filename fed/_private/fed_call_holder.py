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

import logging

# Set config in the very beginning to avoid being overwritten by other packages.
logging.basicConfig(level=logging.INFO)

from fed._private.global_context import get_global_context
from fed.proxy.barriers import send
from fed.fed_object import FedObject
from fed.utils import resolve_dependencies
from fed.tree_util import tree_flatten
import fed.config as fed_config

logger = logging.getLogger(__name__)


class FedCallHolder:
    """
    `FedCallHolder` represents a call node holder when submitting tasks.
    For example,

    f.party("ALICE").remote()
    ~~~~~~~~~~~~~~~~
        ^
        |
    it's a holder.

    """

    def __init__(
        self,
        node_party,
        submit_ray_task_func,
        options={},
    ) -> None:
        self._party = fed_config.get_cluster_config().current_party
        self._node_party = node_party
        self._options = options
        self._submit_ray_task_func = submit_ray_task_func

    def options(self, **options):
        self._options = options
        return self

    def internal_remote(self, *args, **kwargs):
        if not self._node_party:
            raise ValueError("You should specify a party name on the fed actor.")

        # Generate a new fed task id for this call.
        fed_task_id = get_global_context().next_seq_id()
        if self._party == self._node_party:
            resolved_args, resolved_kwargs = resolve_dependencies(
                self._party, fed_task_id, *args, **kwargs
            )
            # TODO(qwang): Handle kwargs.
            ray_obj_ref = self._submit_ray_task_func(resolved_args, resolved_kwargs)
            if isinstance(ray_obj_ref, list):
                return [
                    FedObject(self._node_party, fed_task_id, ref, i)
                    for i, ref in enumerate(ray_obj_ref)
                ]
            else:
                return FedObject(self._node_party, fed_task_id, ray_obj_ref)
        else:
            flattened_args, _ = tree_flatten((args, kwargs))
            for arg in flattened_args:
                # TODO(qwang): We still need to cosider kwargs and a deeply object_ref
                # in this party.
                if isinstance(arg, FedObject) and arg.get_party() == self._party:
                    if arg._was_sending_or_sent_to_party(self._node_party):
                        # This object was sending or sent to the target party, so no
                        # need to do it again.
                        continue
                    else:
                        arg._mark_is_sending_to_party(self._node_party)
                        send(
                            dest_party=self._node_party,
                            data=arg.get_ray_object_ref(),
                            upstream_seq_id=arg.get_fed_task_id(),
                            downstream_seq_id=fed_task_id,
                        )
            if (
                self._options
                and 'num_returns' in self._options
                and self._options['num_returns'] > 1
            ):
                num_returns = self._options['num_returns']
                return [
                    FedObject(self._node_party, fed_task_id, None, i)
                    for i in range(num_returns)
                ]
            else:
                return FedObject(self._node_party, fed_task_id, None)
