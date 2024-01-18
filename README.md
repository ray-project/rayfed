# RayFed
![docs building](https://readthedocs.org/projects/rayfed/badge/?version=latest) ![test on many rays](https://github.com/ray-project/rayfed/actions/workflows/unit_tests_on_ray_matrix.yml/badge.svg) ![test on ray 1.13.0](https://github.com/ray-project/rayfed/actions/workflows/test_on_ray1.13.0.yml/badge.svg)

A multiple parties joint, distributed execution engine based on Ray, to help build your own federated learning frameworks in minutes.

## Overview
**Note: This project is now in actively developing.**

RayFed is a distributed computing framework for cross-parties federated learning.
Built in the Ray ecosystem, RayFed provides a Ray native programming pattern for federated learning so that users can build a distributed program easily.

It provides users the role of "party", thus users can write code belonging to the specific party explicitly imposing more clear data perimeters. These codes will be restricted to execute within the party.

As for the code execution, RayFed introduces the multi-controller architecture:
The code view in each party is exactly the same, but the execution differs based on the declared party of code and the current party of executor. 



## Features
- **Ray Native Programming Pattern**  
  
  Let you write your federated and distributed computing applications like a single-machine program.

- **Multiple Controller Execution Mode**  
  
  The RayFed job can be run in the single-controller mode for developing and debugging and the multiple-controller mode for production without code change.
  
- **Very Restricted and Clear Data Perimeters**  
  
  Because of the PUSH-BASED data transferring mechanism and multiple controller execution mode, the data transmission authority is held by the data owner rather than the data demander.

- **Very Large Scale Federated Computing and Training**  
  
  Powered by the scalabilities and the distributed abilities from Ray, large scale federated computing and training jobs are naturally supported.


## Supported Ray Versions
| RayFed Versions | ray-1.13.0 | ray-2.0.0 | ray-2.1.0 | ray-2.2.0 | ray-2.3.0 | ray-2.4.0 |
|:---------------:|:--------:|:--------:|:--------:|:--------:|:--------:|:--------:|
| 0.1.0           |✅      | ✅      | ✅      | ✅      | ✅      | ✅      |
| 0.2.0           |not released|not released|not released|not released|not released|not released|


## Installation
Install it from pypi.

```shell
pip install -U rayfed
```

Install the nightly released version from pypi.

```shell
pip install -U rayfed-nightly
```
## Quick Start

This example shows how to aggregate values across two participators.

### Step 1: Write an Actor that Generates Value
The `MyActor` increment its value by `num`. 
This actor will be executed within the explicitly declared party.

```python
import sys
import ray
import fed

@fed.remote
class MyActor:
    def __init__(self, value):
        self.value = value

    def inc(self, num):
        self.value = self.value + num
        return self.value
```
### Step 2: Define Aggregation Function
The below function collects and aggragates values from two parties separately, and will also be executed within the declared party.

```python
@fed.remote
def aggregate(val1, val2):
    return val1 + val2
```

### Step 3: Create the actor and call methods in a specific party

The creation code is similar with `Ray`, however, the difference is that in `RayFed` the actor must be explicitly created within a party:

```python
actor_alice = MyActor.party("alice").remote(1)
actor_bob = MyActor.party("bob").remote(1)

val_alice = actor_alice.inc.remote(1)
val_bob = actor_bob.inc.remote(2)

sum_val_obj = aggregate.party("bob").remote(val_alice, val_bob)
```
The above codes:
1. Create two `MyActor`s separately in each party, i.e. 'alice' and 'bob';
2. Increment by '1' in alice and '2' in 'bob';
3. Execute the aggregation function in party 'bob'.

### Step 4: Declare Cross-party Cluster & Init 
```python
def main(party):
    ray.init(address='local')

    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party)
```
This first declares a two-party cluster, whose addresses corresponding to '127.0.0.1:11012' in 'alice' and '127.0.0.1:11011' in 'bob'.
And then, the `fed.init` create a cluster in the specified party.
Note that `fed.init` should be called twice, passing in the different party each time.

When executing codes in step 1~3, the 'alice' cluster will only execute functions whose "party" are also declared as 'alice'.

### Put it together !
Save below codes as `demo.py`: 
```python
import sys
import ray
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
    ray.init(address='local')

    addresses = {
        'alice': '127.0.0.1:11012',
        'bob': '127.0.0.1:11011',
    }
    fed.init(addresses=addresses, party=party)

    actor_alice = MyActor.party("alice").remote(1)
    actor_bob = MyActor.party("bob").remote(1)

    val_alice = actor_alice.inc.remote(1)
    val_bob = actor_bob.inc.remote(2)

    sum_val_obj = aggregate.party("bob").remote(val_alice, val_bob)
    result = fed.get(sum_val_obj)
    print(f"The result in party {party} is {result}")

    fed.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    assert len(sys.argv) == 2, 'Please run this script with party.'
    main(sys.argv[1])

```

### Run The Code.

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

Figure shows the execution under the hood:
<div align="center">
<img src="https://s1.ax1x.com/2023/03/08/ppeH68x.png" alt="Figure" width="500">  
</div>
## Running untrusted codes
As a general rule: Always execute untrusted codes inside a sandbox (e.g., [nsjail](https://github.com/google/nsjail)).

## Who use us
[![][morse-logo-image]][morse-url]
[![][secretflow-logo-image]][secretflow-url]

[morse-logo-image]: docs/images/morse-logo.png
[secretflow-logo-image]: docs/images/secretflow-logo.png
[morse-url]: https://github.com/alipay/Antchain-MPC
[secretflow-url]: https://github.com/secretflow/secretflow
