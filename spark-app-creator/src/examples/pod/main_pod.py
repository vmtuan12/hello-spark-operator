import logging

import yaml
from k8s_manipulators.client import PodClient
from kubernetes.client.models import V1Pod
from utils.k8s_utils import MyDeserializer
from utils.logging_setup import setup_logging

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting ...")

    with open("examples/pod/example_pod.yaml", "r") as f:
        data = yaml.safe_load(f)

    deserializer = MyDeserializer()
    pod_body: V1Pod = deserializer.deserialize_data(data, V1Pod)

    client = PodClient(
        pod=pod_body,
        is_client_outside_cluster=True,
        context="minikube"
    )
    client.run_pod()
    
    logger.info("Finished")

if __name__ == "__main__":
    setup_logging()
    main()
