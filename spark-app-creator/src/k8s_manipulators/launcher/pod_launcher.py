import kubernetes
from kubernetes.client.rest import ApiException
from urllib3.exceptions import (
    ConnectionError, IncompleteRead, ProtocolError
)
from time import sleep

from typing import Generator
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1Pod, V1ObjectMeta
from k8s_manipulators.launcher import BaseLauncher

class PodLauncher(BaseLauncher):
    def __init__(self, api_client: ApiClient) -> None:
        super().__init__(api_client)

    def create_pod(self, namespace: str, pod: V1Pod | dict) -> None:
        if isinstance(pod, V1Pod):
            body = self.core_v1_api.api_client.sanitize_for_serialization(pod)
        else:
            body = pod

        self.logger.info("Creating pod in namespace %s ..." % namespace)

        self.core_v1_api.create_namespaced_pod(
            namespace=namespace,
            body=body,
        )

        self.logger.info("Finished creating pod in namespace %s." % namespace)


    def monitor_pod(self, pod: V1Pod) -> None:
        for yielded_pod in self.monitor_pod_status(pod=pod):
            yielded_pod_metadata: V1ObjectMeta = yielded_pod.metadata
            yielded_pod_namespace = yielded_pod_metadata.namespace
            yielded_pod_name = yielded_pod_metadata.name

            for line in self.read_pod_log(pod=yielded_pod):
                self.logger.info("Pod %s - Namespace %s | %s" % (yielded_pod_name, yielded_pod_namespace, line.decode().strip()))

        self.logger.info("Pod %s - Namespace %s | Finished!" % (yielded_pod_name, yielded_pod_namespace))


    def monitor_pod_status(self, pod: V1Pod) -> Generator[V1Pod, None, None]:
        pod_metadata: V1ObjectMeta = pod.metadata
        pod_namespace = pod_metadata.namespace
        pod_name = pod_metadata.name

        _w = kubernetes.watch.Watch()
        while True:
            try:
                for event in _w.stream(
                    self.core_v1_api.list_namespaced_pod,
                    namespace=pod_namespace,
                    field_selector=f"metadata.name={pod_name}",
                ):
                    event_type = event['type']
                    pod_obj = event["object"]
                    phase = pod.status.phase

                    self.logger.info(
                        "Pod %s - Namespace %s | Event type: %s - Phase: " % (
                        event['object'].metadata.name, event['object'].metadata.namespace, event_type, phase
                    ))

                    if phase in ("Succeeded", "Failed") or event_type in ("DELETE", "ERROR"):
                        return

                    yield pod_obj
    
            except ApiException as e:
                if e.status != 410:
                    raise
                # https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
                self.logger.warning("Kubernetes ApiException 410 (Gone): %s", e.reason)
                self.logger.warning("Let's retry w/ most recent event")

            except (ProtocolError, ConnectionError, IncompleteRead) as e:
                if connection_retry_attempt == 0:
                    raise

                self.logger.warning("Unexpected Kubernetes connection error: %s", e)

                connection_retry_attempt -= 1
                sleep(1)

                self.logger.warning("Let's retry w/ most recent event. Attempt: %s/10", str(10 - connection_retry_attempt))


