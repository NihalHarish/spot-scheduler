import random

from kubernetes import client, config, watch


SPOT_LABEL = 'spot'

config.load_kube_config('./config-spark-on-cat')
v1 = client.CoreV1Api()

def nodes_available():
    ready_nodes = []
    for node in v1.list_node().items:
        for status in node.status.conditions:
            if status.status == "True" and status.type == "Ready":
                ready_nodes.append(node)
    return ready_nodes


def get_available_spot_nodes():
    available_nodes = nodes_available()
    spot_nodes = []
    for node in available_nodes:
        if SPOT_LABEL in node.metadata.labels:
            spot_nodes.append(node)
    return spot_nodes


def get_available_persistent_nodes():
    available_nodes = nodes_available()
    persistent_nodes = []
    for node in available_nodes:
        if SPOT_LABEL not in node.metadata.labels:
            persistent_nodes.append(node)
    return persistent_nodes


def choose_random_node(nodes):
    return random.choice(nodes)


def is_driver_pod(pod):
    spark_role = pod.metadata.labels['spark_role']
    return spark_role == 'driver'


def spot_over_non_spot_always(pod):
    '''
        Chooses a spot node always except
        if the requesting pod is a driver
    '''

    if is_driver_pod(pod):
        persistent_nodes = get_available_persistent_nodes()
        return choose_random_node(persistent_nodes)
    else:
        spot_nodes = get_available_spot_nodes()
        return choose_random_node(spot_nodes)
