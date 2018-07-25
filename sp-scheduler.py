import time
import random
import json
import numpy as np
import kubernetes.client

from kubernetes import client, config, watch
from heuristics import spot_over_non_spot_always

config.load_kube_config('./config-spark-on-cat')
v1=client.CoreV1Api()

scheduler_name = "spot-scheduler"
spot_percent = float("0.8")

def get_volatility_metrics(instance_name):
    pass

def scheduler(name, node, namespace="default"):
    target=client.V1ObjectReference()
    target.kind="Node"
    target.apiVersion="v1"
    target.name= node
    meta=client.V1ObjectMeta()
    meta.name=name
    body=client.V1Binding(target=target, metadata=meta)
    body.target=target
    body.metadata=meta
    return v1.create_namespaced_binding(namespace, body)

def main():
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, "default"):
        if event['object'].status.phase == "Pending" and event['object'].spec.scheduler_name == scheduler_name:
            try:
                requesting_pod = event['object']
                pod_name = event['object'].metadata.name
                selected_node = spot_over_non_spot_always(requesting_pod).metadata.name
                res = scheduler(pod_name, selected_node)
            except client.rest.ApiException as e:
                pass
            except ValueError as ve:
                pass

if __name__ == '__main__':
    main()
