import kubernetes
from custom_exceptions import (PodFailedException,
                               ResourceObjectNotFoundException,
                               SparkAppFailedException,
                               SparkAppSubmissionFailedException)
from k8s_manipulators.client import BaseClient
from k8s_manipulators.launcher import SparkAppLauncher
from k8s_objects.spark_app import SparkApp
from kubernetes.client.models import V1ObjectMeta, V1Pod


class SparkAppClient(BaseClient):
    def __init__(self, spark_app: SparkApp, **kwargs) -> None:
        super().__init__(**kwargs)
        self.launcher = SparkAppLauncher(self.api_client)
        self.spark_app = spark_app


    def run_spark_app(self, namespace: str = None, cleanup_on_failure: bool = True):
        if namespace:
            spark_app_namespace = namespace
        else:
            spark_app_metadata: V1ObjectMeta = self.spark_app.metadata
            spark_app_namespace = spark_app_metadata.namespace

        if not spark_app_namespace:
            self.logger.error("Must define namespace for SparkApp %s" % spark_app_metadata.name)
            raise ValueError("Must define namespace for SparkApp %s" % spark_app_metadata.name)
        
        if self.hooks is not None:
            self._execute_hooks()

        self.launcher.create_spark_app(namespace=spark_app_namespace, spark_app=self.spark_app)

        try:
            self.launcher.monitor_spark_app(spark_app=self.spark_app)
        except (SparkAppFailedException, SparkAppSubmissionFailedException):
            if cleanup_on_failure:
                self.logger.info(
                    "SparkApp %s - Namespace %s | SparkApp failed, cleaning up ..." % (
                    spark_app_metadata.name, spark_app_metadata.namespace
                ))
                self._clean_up()
            raise

        self.logger.info(
            "SparkApp %s - Namespace %s | SparkApp finished running, cleaning up ..." % (
            spark_app_metadata.name, spark_app_metadata.namespace
        ))
        self._clean_up()


    def _execute_hooks(self):
        for hook in self.hooks:
            hook(spark_app=self.spark_app)

    
    def _clean_up(self):
        try:
            self.launcher.delete_spark_app(self.spark_app)
        except ResourceObjectNotFoundException as e:
            pass
        
        self.logger.info("Cleaned up! Everything done aweeeeeesomely")
