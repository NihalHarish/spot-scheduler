import random

from kubernetes import client, config, watch

from aws_metrics import get_instance_volatility

SPOT_LABEL = 'spot-instance'

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
        if SPOT_LABEL in node.metadata.labels and node.metadata.labels[SPOT_LABEL] == 'true':
            spot_nodes.append(node)
    return spot_nodes


def get_available_persistent_nodes():
    available_nodes = nodes_available()
    persistent_nodes = []
    for node in available_nodes:
        if SPOT_LABEL not in node.metadata.labels or node.metadata.labels[SPOT_LABEL] == 'false':
            persistent_nodes.append(node)
    return persistent_nodes


def choose_random_node(nodes):
    return random.choice(nodes)


def is_driver_pod(pod):
    spark_role = pod.metadata.labels['spark-role']
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

def get_instance_type(node):
    '''
        In a managed cluster the instance_type will be stored
        in the label beta.kubernetes.io/instance-type
    '''
    return node.metadata.labels['beta.kubernetes.io/instance-type']


def get_instance_bid_price(instance_type):
    '''
        Stub:  Need to implement mechanism to obtain bid price
    '''
    return 0.10


def get_instance_region(node):
    return node.metadata.labels['failure-domain.beta.kubernetes.io/region']


def get_instance_zone(node):
    return node.metadata.labels['failure-domain.beta.kubernetes.io/zone']


def get_node_volatilty(node):
    instance_type = get_instance_type(node)
    bid = get_instance_bid_price(instance_type)
    time_span = 5
    product_choice = "Linux/UNIX"
    return get_instance_volatility(instance_type, bid, time_span, product_choice)


def least_volatile_nodes_always(pod):
    '''
        Chooses the least volatile spot node

    '''
    if is_driver_pod(pod):
        persistent_nodes = get_available_persistent_nodes()
        return choose_random_node(persistent_nodes)
    else:
        volatility_map = {} #Groups nodes by volitilty. key: volatility value = [] node_list
        spot_nodes = get_available_spot_nodes()
        volatility_values = []
        for node in spot_nodes:
            volatility = get_node_volatilty(node)
            if volatility in volatility_map:
                volatility_map[volatility].append(node)
            else:
                volatility_values.append(volatility)
                volatility_map[volatility] = [node]
        return choose_random_node(volatility_map[min(volatility_values)])
