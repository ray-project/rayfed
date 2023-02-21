# RayFed
A multiple parties joint, distributed execution engine based on Ray, to help build your own federated learning frameworks in minutes.


## Features
- Ray native programming pattern
  Write your federated computing applications like writing a single meachine program.
- Multiple Controller Execution Mode:
  
- Very restricted and clear data perimeters
  
- Very large scale federated computing or training
  

## Supported Ray Versions
| RayFed Versions | ray2.0.0 | ray2.1.0 | ray2.2.0 | ray2.3.0 |
|:---------------:|:--------:|:--------:|:--------:|:--------:|
| 0.1.0           |✅      | ✅      | ✅      | ✅      |
| 0.2.0           |          |          |          |          |


## Installation
Install it from pypi.

```shell
pip install -U rayfed
```

## Quick Start

This example shows how to do aggregation across two participators.

### Step 1: save the code.
Save the code as a python file, e.g., test.py.

```python
import sys

import fed


@fed.remote
class MyActor:
    def __init__(self, value):
        self.value = value

    def inc(self, num):
        self.value = self.value + num
        return self.value


@fed.remote
def aggregate(val1, val2):
    return val1 + val2


def main(party):
    cluster = {
        'alice': {'address': '127.0.0.1:11010'},
        'bob': {'address': '127.0.0.1:11011'},
    }
    fed.init(address='local', cluster=cluster, party=party)

    actor_alice = MyActor.party("alice").remote(1)
    actor_bob = MyActor.party("bob").remote(1)

    val_alice = actor_alice.inc.remote(1)
    val_bob = actor_bob.inc.remote(2)

    sum_val_obj = aggregate.party("bob").remote(val_alice, val_bob)
    result = fed.get(sum_val_obj)
    print(f"The result in party {party} is {result}")

    fed.shutdown()


if __name__ == "__main__":
    assert len(sys.argv) == 2, 'Please run this script with party.'
    main(sys.argv[1])

```

### Step 2: run the code.

Open a terminal and run the code as `alice`. It's recommended to run the code with Ray TLS enabled (please refer to [Ray TLS](https://docs.ray.io/en/latest/ray-core/configure.html#tls-authentication))
```shell
RAY_USE_TLS=1 \
RAY_TLS_SERVER_CERT='/path/to/the/server/cert/file' \
RAY_TLS_SERVER_KEY='/path/to/the/server/key/file' \
RAY_TLS_CA_CERT='/path/to/the/ca/cert/file' \
python test.py alice
```

In the mean time, open another terminal and run the code as `bob`.
```shell
RAY_USE_TLS=1 \
RAY_TLS_SERVER_CERT='/path/to/the/server/cert/file' \
RAY_TLS_SERVER_KEY='/path/to/the/server/key/file' \
RAY_TLS_CA_CERT='/path/to/the/ca/cert/file' \
python test.py bob
```

Then you will get `The result in party alice is 5` on the first terminal screen and `The result in party bob is 5` on the second terminal screen.

## Running untrusted codes
As a general rule: Always execute untrusted codes inside a sandbox (e.g., [nsjail](https://github.com/google/nsjail)).