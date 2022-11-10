# RayFed
A multiple parties joint, distributed execution engine on the top of Ray.

## Quick Start
Firstly, install essential dependencies by running the following commands in project's root dir:

```shell
> pip install -r requirements.txt
> pip install -e . # install rayfed
```

Run the demo program:
```shell
> python examples/test_fl.py
```

## How to use?
```python
# Federated learning across 2 parties on Ray.

@fed.remote
class MyModel:
    pass

@fed.remote(party="ALICE)
class Aggregator:
    pass

model_1 = MyModel.party("ALICE").remote()
model_2 = MyModel.party("BOB").remote()
aggregator = Aggregator.remote()

model_1.load_data.remote()
model_2.load_data.remote()

for epoch in range(num_epochs):
    # 1. Train local model in the local parties.
    for step in range(num_steps):
        model1.train.remote()
        model2.train.remote()

    # 2. Aggregrate gradients and re-distribute it.
    acc1, w1 = model1.get_weights.remote()
    acc2, w2 = model2.get_weights.remote()
    new_w = aggregator.mean.remote(w1, w2)
    model1.update_weights.remote(new_w)
    model2.update_weights.remote(new_w)

    # step3: Aggregrate metrics and early stop.
    meant_acc = aggregator.mean.remote(acc1, acc2)
    if fed.get(meant_acc) > 0.98:
        break
```
