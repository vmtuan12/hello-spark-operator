from time import sleep
from typing import Generator

import kubernetes
from k8s_manipulators.launcher import BaseLauncher
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1ObjectMeta, V1Pod
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ConnectionError, IncompleteRead, ProtocolError
from utils.k8s_utils import get_pod_status_phase, PodStatusPhaseEnum as PodStatusPhase, PodEventTypeEnum as PodEventType
from custom_exceptions import PodFailedException, ResourceObjectNotFoundException, PermissionDeniedException


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
        for yielded_pod in self._monitor_pod_status(pod=pod):
            yielded_pod_metadata: V1ObjectMeta = yielded_pod.metadata
            yielded_pod_namespace = yielded_pod_metadata.namespace
            yielded_pod_name = yielded_pod_metadata.name

            log_prefix = "Pod %s - Namespace %s" % (yielded_pod_name, yielded_pod_namespace)
            phase = get_pod_status_phase(pod=yielded_pod)

            if phase not in (PodStatusPhase.RUNNING.value, PodStatusPhase.SUCCEEDED.value, PodStatusPhase.FAILED.value):
                continue

            for line in self._read_pod_log(pod=yielded_pod):
                self.logger.info("%s | %s" % (log_prefix, line.decode().strip()))

            if phase == PodStatusPhase.FAILED.value:
                self.logger.info("%s | Pod failed!" % log_prefix)
                raise PodFailedException()

        self.logger.info("%s | Finished monitoring!" % log_prefix)


    def delete_pod(self, pod: V1Pod, **kwargs) -> None:
        pod_metadata: V1ObjectMeta = pod.metadata
        pod_namespace = pod_metadata.namespace
        pod_name = pod_metadata.name
        
        try:
            self.core_v1_api.delete_namespaced_pod(name=pod_name, namespace=pod_namespace, **kwargs)
            self.logger.info("Deleted pod %s in namespaced %s successfully" % (pod_name, pod_namespace))
        except ApiException as e:
            if e.status == 404:
                raise ResourceObjectNotFoundException(
                    resource_type="pod", 
                    message="Pod %s in namespaced %s - not found" % (pod_name, pod_namespace)
                )
            
            if e.status == 403:
                raise PermissionDeniedException(
                    "Do not have enough permission to delete pod %s in namespace %s" % (pod_name, pod_namespace)
                )
            
            raise e


    def _monitor_pod_status(self, pod: V1Pod) -> Generator[V1Pod, None, None]:
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
                    pod_obj: V1Pod = event["object"]
                    phase = get_pod_status_phase(pod=pod_obj)

                    self.logger.info(
                        "Pod %s - Namespace %s | Event type: %s - Phase: %s" % (
                        event['object'].metadata.name, event['object'].metadata.namespace, event_type, phase
                    ))

                    if (phase in (PodStatusPhase.FAILED.value, PodStatusPhase.SUCCEEDED.value) or 
                        event_type in (PodEventType.DELETE.value, PodEventType.ERROR.value)):
                        yield pod_obj
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


