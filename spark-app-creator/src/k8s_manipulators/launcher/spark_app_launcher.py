from time import sleep
from typing import Generator

import kubernetes
from custom_exceptions import (PermissionDeniedException, PodFailedException,
                               ResourceObjectNotFoundException)
from k8s_manipulators.launcher import BaseLauncher
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1ObjectMeta, V1Pod
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ConnectionError, IncompleteRead, ProtocolError
from utils.k8s_utils import PodEventTypeEnum as PodEventType
from utils.k8s_utils import PodStatusPhaseEnum as PodStatusPhase
from utils.k8s_utils import get_pod_status_phase

from k8s_objects.spark_app import SparkApp 
from utils import consts


class SparkAppLauncher(BaseLauncher):
    def __init__(self, api_client: ApiClient) -> None:
        super().__init__(api_client)
        self.custom_object_api = kubernetes.client.CustomObjectsApi(api_client=api_client)


    def create_spark_app(self, namespace: str, spark_app: SparkApp | dict) -> None:
        if isinstance(spark_app, SparkApp):
            body = self.custom_object_api.api_client.sanitize_for_serialization(spark_app)
        else:
            body = spark_app

        self.logger.info("Creating SparkApplication %s in namespace %s ..." % (spark_app.metadata.name, namespace))

        self.custom_object_api.create_namespaced_custom_object(
            group=consts.SPARK_APP_GROUP,
            version=consts.SPARK_APP_VERSION,
            plural=consts.SPARK_APP_PLURAL,
            namespace=namespace,
            body=body,
        )

        self.logger.info("Finished creating SparkApplication %s in namespace %s." % (spark_app.metadata.name, namespace))


    def _monitor_spark_driver_status(self, spark_app: SparkApp) -> Generator[V1Pod, None, None]:
        spark_app_metadata: V1ObjectMeta = spark_app.metadata
        spark_app_namespace = spark_app_metadata.namespace
        spark_app_name = spark_app_metadata.name

        if spark_app.spec.driver.pod_name is not None:
            driver_pod_name = spark_app.spec.driver.pod_name
        else:
            driver_pod_name = f"{spark_app_name}-driver"

        _w = kubernetes.watch.Watch()
        connection_retry_attempt = 0
        while True:
            try:
                for event in _w.stream(
                    self.core_v1_api.list_namespaced_pod,
                    namespace=spark_app_namespace,
                    field_selector=f"metadata.name={driver_pod_name}",
                ):
                    event_type = event['type']
                    pod_obj: V1Pod = event["object"]
                    phase = get_pod_status_phase(pod=pod_obj)

                    self.logger.info(
                        "Driver %s - Namespace %s | Event type: %s - Phase: %s" % (
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

