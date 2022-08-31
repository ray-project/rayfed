# RayFed
A multiple parties involved execution engine on the top of Ray.  

## How to use?
```python
# Federated learning across 3 parties.

@ray.remote
def length(x):
    return len(x)

@ray.fed
class MyModel:
    def fit(self, x):
        pass

@ray.fed(party="CAROL")
class Aggregator:
    pass

x_alice = read_cvs.party("ALICE").bind("alice.csv")
x_bob = read_csv.party("BOB").bind("bob.csv")
actor_alice = MyModel.party("ALICE").bind(x_alice)
actor_bob = MyModel.party("BOB").bind(x_bob)
aggregator = Aggregator.bind()

n_alice = length.party("ALICE").bind(x_alice)
n_bob = length.party("BOB").bind(x_bob)

n_alice_ref = n_alice.execute()
n_bob_ref = n_bob.execute()

na, nb = ray.get([n_alice_ref, n_bob_ref])
step_per_epochs = min(na, nb) # batch_size


for epoch in range(10):
    for step in range(500):
        # step1: Train in local parties.
        g_step = epoch * step_per_epochs + step
        metrics_alice, weight_alice = actor_alice.fit.bind(g_step, current_weight)
        metrics_bob, weight_bob = actor_bob.fit.bind(g_step, current_weight)

        # step 2: grads aggregrating.
        current_weight = aggregator.average.bind(weight_alice, weight_bob)
        actor_alice.update.bind(current_weight)
        actor_bob.update.bind(current_weight)

        # step 3: metrics aggregrating, early stop.
        global_metrics = aggregator.metrics_average.bind(metrics_alice, metrics_bob)
        g_metrics = ray.get(global_metrics.execute())
        if g_metrics < 0.8:
            break

```
