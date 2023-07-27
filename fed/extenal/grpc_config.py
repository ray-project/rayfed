


"""This module should be cached locally due to all configurations
   are mutable.
"""

import fed._private.compatible_utils as compatible_utils
import fed._private.constants as fed_constants
import cloudpickle
import json

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class GrpcCrossSiloMessageConfig:
    """A class to store parameters used for GRPC communication

    Attributes:
        grpc_retry_policy: a dict descibes the retry policy for
            cross silo rpc call. If None, the following default retry policy
            will be used. More details please refer to
            `retry-policy <https://github.com/grpc/proposal/blob/master/A6-client-retries.md#retry-policy>`_. # noqa

            .. code:: python
                {
                    "maxAttempts": 4,
                    "initialBackoff": "0.1s",
                    "maxBackoff": "1s",
                    "backoffMultiplier": 2,
                    "retryableStatusCodes": [
                        "UNAVAILABLE"
                    ]
                }
        grpc_channel_options: A list of tuples to store GRPC channel options,
            e.g. [
                    ('grpc.enable_retries', 1),
                    ('grpc.max_send_message_length', 50 * 1024 * 1024)
                ]
    """
    grpc_channel_options: List = None
    grpc_retry_policy: Dict[str, str] = None


    @classmethod
    def from_dict(cls, data: Dict):
        """Initialize CrossSiloMessageConfig from a dictionary.

        Args:
            data (Dict): Dictionary with keys as member variable names.

        Returns:
            CrossSiloMessageConfig: An instance of CrossSiloMessageConfig.
        """
        # Get the attributes of the class

        data = data or {}
        all_annotations = cls.__annotations__
        # all_annotations = {**cls.__annotations__, **cls.__base__.__annotations__}
        attrs = {attr for attr, _ in all_annotations.items()}
        # Filter the dictionary to only include keys that are attributes of the class
        filtered_data = {key: value for key, value in data.items() if key in attrs}
        return cls(**filtered_data)
