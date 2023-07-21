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

import ray
from fed._private.fed_call_holder import FedCallHolder
from fed.fed_object import FedObject

logger = logging.getLogger(__name__)


class FedActorHandle:
    def __init__(
        self,
        fed_class_task_id,
        addresses,
        cls,
        party,
        node_party,
        options,
    ) -> None:
        self._fed_class_task_id = fed_class_task_id
        self._addresses = addresses
        self._body = cls
        self._party = party
        self._node_party = node_party
        self._options = options
        self._actor_handle = None

    def __getattr__(self, method_name: str):
        # User trying to call .bind() without a bind class method
        if method_name == "remote" and "remote" not in dir(self._body):
            raise AttributeError(f".remote() cannot be used again on {type(self)} ")
        # Raise an error if the method is invalid.
        getattr(self._body, method_name)
        call_node = FedActorMethod(
            self._addresses,
            self._party,
            self._node_party,
            self,
            method_name,
        ).options(**self._options)
        return call_node

    def _execute_impl(self, cls_args, cls_kwargs):
        """Executor of ClassNode by ray.remote()

        Args and kwargs are to match base class signature, but not in the
        implementation. All args and kwargs should be resolved and replaced
        with value in bound_args and bound_kwargs via bottom-up recursion when
        current node is executed.
        """
        if self._node_party == self._party:
            self._actor_handle = (
                ray.remote(self._body)
                .options(**self._options)
                .remote(*cls_args, **cls_kwargs)
            )

    def _execute_remote_method(self, method_name, options, args, kwargs):
        num_returns = 1
        if options and 'num_returns' in options:
            num_returns = options['num_returns']
        logger.debug(
            f"Actor method call: {method_name}, num_returns: {num_returns}"
        )
        ray_object_ref = self._actor_handle._actor_method_call(
            method_name,
            args=args,
            kwargs=kwargs,
            name="",
            num_returns=num_returns,
            concurrency_group_name="",
        )
        return ray_object_ref


class FedActorMethod:
    def __init__(
        self,
        addresses,
        party,
        node_party,
        fed_actor_handle,
        method_name,
    ) -> None:
        self._addresses = addresses
        self._party = party  # Current party
        self._node_party = node_party
        self._fed_actor_handle = fed_actor_handle
        self._method_name = method_name
        self._options = {}
        self._fed_call_holder = FedCallHolder(node_party, self._execute_impl)

    def remote(self, *args, **kwargs) -> FedObject:
        return self._fed_call_holder.internal_remote(*args, **kwargs)

    def options(self, **options):
        self._options = options
        self._fed_call_holder.options(**options)
        return self

    def _execute_impl(self, args, kwargs):
        return self._fed_actor_handle._execute_remote_method(
            self._method_name, self._options, args, kwargs
        )
