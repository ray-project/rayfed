# An example to a horizontal federated deep learning which is involved in 2 parties.

import ray

epochs = 5
batch_size = 1024

x_alice = read_csv.options(resources={"RES_ALICE", 1}).bind("alice.csv")
x_bob = read_csv.options(resources={"RES_BOB", 1}).bind("bob.csv")
actor_alice = Model.options(resources={"RES_ALICE", 1}).bind(x_alice)
actor_bob = Model.options(resources={"RES_BOB", 1}).bind(x_bob)
aggregator = Aggregator.options(resources={"RES_CAROL", 1}).bind()


@ray.remote
def length(x):
    return len(x)

n_alice = length.options(resources={"RES_ALICE", 1}).bind(x_alice)
n_bob = length.options(resources={"RES_BOB", 1}).bind(x_bob)

n_alice_ref = n_alice.execute()
n_bob_ref = n_bob.execute()

na, nb = ray.get([n_alice_ref, n_bob_ref])
step_per_epochs = min(na, nb) // batch_size


for epoch in range(epochs):
    for step in range(step_per_epochs):
        # step 1: Train in different parties.
        g_step = epoch * step_per_epochs + step
        metrics_alice, weight_alice = actor_alice.fit.bind(g_step, current_weight)
        metrics_bob, weight_bob = actor_bob.fit.bind(g_step, current_weight)

        # step 2: Aggregate gradients for different parties.
        current_weight = aggregator.average.bind(weight_alice, weight_bob)
        actor_alice.update.bind(current_weight)
        actor_bob.update.bind(current_weight)

        # step 3: Aggregate metrics and early stop.
        global_metrics = aggregator.metrics_average.bind(metrics_alice, metrics_bob)
        g_metrics = ray.get(global_metrics.execute())
        if g_metrics < 0.8:
            break
