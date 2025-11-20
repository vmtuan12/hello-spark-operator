from time import sleep
from typing import Generator

import k8s_objects.spark_app
import kubernetes
from custom_exceptions import (PermissionDeniedException,
                               ResourceObjectNotFoundException,
                               SparkAppFailedException,
                               SparkAppSubmissionFailedException)
from k8s_manipulators.launcher import BaseLauncher
from k8s_objects.spark_app import SparkApp
from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1ObjectMeta, V1Pod
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ConnectionError, IncompleteRead, ProtocolError
from utils import consts
from utils.k8s_utils import MyDeserializer
from utils.k8s_utils import PodStatusPhaseEnum as PodStatusPhase
from utils.k8s_utils import SparkApplicationStateEnum as SparkAppState
from utils.k8s_utils import get_pod_status_phase


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

    
    def monitor_spark_app(self, spark_app: SparkApp):
        spark_app_metadata: V1ObjectMeta = spark_app.metadata
        spark_app_namespace = spark_app_metadata.namespace
        spark_app_name = spark_app_metadata.name

        if spark_app.spec.driver.pod_name is not None:
            driver_pod_name = spark_app.spec.driver.pod_name
        else:
            driver_pod_name = f"{spark_app_name}-driver"

        log_prefix = "SparkApp %s - Namespace %s - Driver" % (spark_app_name, spark_app_namespace)

        for yielded_spark_app in self._monitor_spark_app_state(spark_app):
            spark_app_status = yielded_spark_app.status
            spark_app_state = spark_app_status.application_state.state

            if spark_app_state in (SparkAppState.PENDING_RERUN, SparkAppState.INVALIDATING, SparkAppState.UNKNOWN):
                continue

            spark_driver_pod = self.core_v1_api.read_namespaced_pod(
                name=driver_pod_name,
                namespace=spark_app_namespace,
            )
            driver_phase = get_pod_status_phase(pod=spark_driver_pod)

            if driver_phase not in (PodStatusPhase.RUNNING.value, PodStatusPhase.SUCCEEDED.value, PodStatusPhase.FAILED.value):
                continue

            for line in self._read_pod_log(pod=spark_driver_pod):
                self.logger.info("%s | %s" % (log_prefix, line.decode().strip()))

            if spark_app_state == SparkAppState.FAILED:
                self.logger.error("%s | Spark Application failed!" % log_prefix)
                raise SparkAppFailedException()
            elif spark_app_state == SparkAppState.SUBMISSION_FAILED:
                self.logger.error("%s | Spark Application submission failed!" % log_prefix)
                raise SparkAppSubmissionFailedException()

        self.logger.info("%s | Finished monitoring!" % log_prefix)

    def delete_spark_app(self, spark_app: SparkApp, **kwargs) -> None:
        spark_app_metadata: V1ObjectMeta = spark_app.metadata
        spark_app_namespace = spark_app_metadata.namespace
        spark_app_name = spark_app_metadata.name
        
        try:
            self.custom_object_api.delete_namespaced_custom_object(
                group=consts.SPARK_APP_GROUP,
                version=consts.SPARK_APP_VERSION,
                plural=consts.SPARK_APP_PLURAL,
                namespace=spark_app_namespace,
                name=spark_app_name,
                **kwargs
            )
            self.logger.info("Deleted SparkApp %s in namespaced %s successfully" % (spark_app_name, spark_app_namespace))
        except ApiException as e:
            if e.status == 404:
                raise ResourceObjectNotFoundException(
                    resource_type="pod", 
                    message="SparkApp %s in namespaced %s - not found" % (spark_app_name, spark_app_namespace)
                )
            
            if e.status == 403:
                raise PermissionDeniedException(
                    "Do not have enough permission to delete pod %s in namespace %s" % (spark_app_name, spark_app_namespace)
                )
            
            raise e

    def _monitor_spark_app_state(self, spark_app: SparkApp) -> Generator[SparkApp, None, None]:
        spark_app_metadata: V1ObjectMeta = spark_app.metadata
        spark_app_namespace = spark_app_metadata.namespace
        spark_app_name = spark_app_metadata.name

        deserializer = MyDeserializer(custom_module=k8s_objects.spark_app)

        _w = kubernetes.watch.Watch()
        connection_retry_attempt = 0
        while True:
            try:
                for event in _w.stream(
                    self.custom_object_api.list_namespaced_custom_object,
                    namespace=spark_app_namespace,
                    group=consts.SPARK_APP_GROUP,
                    version=consts.SPARK_APP_VERSION,
                    plural=consts.SPARK_APP_PLURAL,
                    field_selector=f"metadata.name={spark_app_name}",
                ):
                    spark_app_obj: SparkApp = deserializer.deserialize_data(event["object"], SparkApp)
                    spark_app_status = spark_app_obj.status
                    if spark_app_status is None or spark_app_status.application_state is None:
                        continue
                    
                    spark_app_state = spark_app_status.application_state.state
                    self.logger.info(
                        "SparkApp %s - Namespace %s | State: %s" % (
                        spark_app_name, spark_app_namespace, spark_app_state
                    ))

                    if spark_app_state in (SparkAppState.FAILED, SparkAppState.SUBMISSION_FAILED, SparkAppState.COMPLETED):
                        yield spark_app_obj
                        return

                    yield spark_app_obj
    
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

