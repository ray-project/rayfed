import multiprocessing
from typing import Any, TypeVar

import click
import numpy as np
import pandas as pd
import ray
import tensorflow as tf
from sklearn.preprocessing import OneHotEncoder
from tensorflow import keras

import fed
from fed.api import set_cluster, set_party
from fed.barriers import start_recv_proxy

@fed.remote
def mean(party, x, y):
    print(f"=======[party:{party}] meaning...")
    return np.mean([x, y], axis=0)

@fed.remote
class MyActor:
    def __init__(self, learning_rate, input_shape, num_classes):
        self._learning_rate = learning_rate
        self._input_shape = input_shape
        self._num_classes = num_classes
        self.count = 0

    def load_data(self, batch_size: int, epochs: int):
        df = pd.read_csv('iris.csv')
        x, y = df.iloc[:, :4].values, df.iloc[:, 4:].values
        encoder = OneHotEncoder(sparse=False)
        y = encoder.fit_transform(y)
        self._dataset = iter(
            tf.data.Dataset.from_tensor_slices((x, y)).batch(batch_size).repeat(epochs)
        )

    def build_model(self):
        self.model = keras.Sequential(
            [
                keras.layers.InputLayer(input_shape=self._input_shape),
                keras.layers.Flatten(),
                keras.layers.Dense(16, activation="relu"),
                keras.layers.Dense(64, activation="relu"),
                keras.layers.Dense(32, activation="relu"),
                keras.layers.Dense(self._num_classes, activation="softmax"),
            ]
        )
        optimizer = tf.keras.optimizers.Adam(learning_rate=self._learning_rate)
        self.model.compile(
            optimizer=optimizer,
            loss=tf.keras.losses.categorical_crossentropy,
            metrics=['accuracy'],
        )

    def train_step(self):
        x, y = next(self._dataset)
        with tf.GradientTape() as tape:
            y_pred = self.model(x, training=True)
            loss = self.model.compiled_loss(
                y,
                y_pred,
                regularization_losses=self.model.losses,
            )

            trainable_vars = self.model.trainable_variables
            gradients = tape.gradient(loss, trainable_vars)
            self.model.optimizer.apply_gradients(zip(gradients, trainable_vars))

    def get_weights(self, party):
        print(f"==========[party: {party}] get_weighting...")
        return self.model.get_weights()

    def set_weights(self, party, weights):
        self.count = self.count + 1
        print(f"==========[party: {party}] current count is {self.count}")
        self.model.set_weights(weights)
        return True


@fed.remote
def agg_fn(obj1, obj2):
    return f"agg-{obj1}-{obj2}"


def run(party):
    cluster = {'alice': '127.0.0.1:11010', 'bob': '127.0.0.1:11011'}
    set_cluster(cluster=cluster)
    set_party(party)

    ray.init(num_cpus=8, log_to_driver=True)
    start_recv_proxy(cluster[party], party)
    print(f"=============party name is {party}")

    epochs = 3
    batch_size = 4
    learning_rate = 0.01
    input_shape = 4
    n_classes = 3


    actor_alice = MyActor.party("alice").remote(learning_rate, input_shape, n_classes)
    actor_bob = MyActor.party("bob").remote(learning_rate, input_shape, n_classes)
    print(type(actor_alice), actor_alice.__dict__)

    actor_alice.load_data.remote(batch_size, epochs)
    actor_bob.load_data.remote(batch_size, epochs)
    actor_alice.build_model.remote()
    actor_bob.build_model.remote()

    num_batchs = int(150 / batch_size)
    for epoch in range(epochs):
        for step in range(num_batchs):
            print(f'Epoch {epoch} step {step}.')
            actor_alice.train_step.remote()
            actor_bob.train_step.remote()

        w_a = actor_alice.get_weights.remote(party)
        w_b = actor_bob.get_weights.remote(party)
        w_mean = mean.party('alice').remote(party, w_a, w_b)
        result = fed.get(w_mean)
        n_wa = actor_alice.set_weights.remote(party, w_mean)
        n_wb = actor_bob.set_weights.remote(party, w_mean)
        print(f'Epoch {epoch} finished, mean is {result}')
        # print(f'[{party}] Epoch {epoch} finished, mean is ...')

    print(f"======= The final result in {party} is ...")

    import time
    time.sleep(100)

    ray.shutdown()


@click.command()
@click.option(
    "--party",
    required=False,
    type=str,
)
def main(party: str):
    p_alice = multiprocessing.Process(target=run, args=('alice',))
    p_bob = multiprocessing.Process(target=run, args=('bob',))
    p_alice.start()
    p_bob.start()
    p_alice.join()
    p_bob.join()


if __name__ == "__main__":
    main()