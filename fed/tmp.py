import ray
import fed
ray.init()

cluster = {
    'alice': {'address': '127.0.0.1:11012'},
    'bob': {'address': '127.0.0.1:11011'},
}
party = 'alice'
fed.init(cluster, party)