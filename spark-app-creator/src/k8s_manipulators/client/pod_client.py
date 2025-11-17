import kubernetes
from custom_exceptions import (PodFailedException,
                               ResourceObjectNotFoundException)
from k8s_manipulators.client import BaseClient
from k8s_manipulators.launcher import PodLauncher
from kubernetes.client.models import V1ObjectMeta, V1Pod


class PodClient(BaseClient):
    def __init__(self, pod: V1Pod, **kwargs) -> None:
        super().__init__(**kwargs)
        self.launcher = PodLauncher(self.api_client)
        self.pod = pod


    def run_pod(self, namespace: str = None, cleanup_on_failure: bool = True):
        if namespace:
            pod_namespace = namespace
        else:
            pod_metadata: V1ObjectMeta = self.pod.metadata
            pod_namespace = pod_metadata.namespace

        if not pod_namespace:
            self.logger.error("Must define namespace for pod %s" % pod_metadata.name)
            raise ValueError("Must define namespace for pod %s" % pod_metadata.name)
        
        if self.hooks is not None:
            self._execute_hooks()

        self.launcher.create_pod(namespace=pod_namespace, pod=self.pod)

        try:
            self.launcher.monitor_pod(pod=self.pod)
        except PodFailedException:
            if cleanup_on_failure:
                self.logger.info(
                    "Pod %s - Namespace %s | Pod failed, cleaning up ..." % (
                    pod_metadata.name, pod_metadata.namespace
                ))
                self._clean_up()
            raise

        self.logger.info(
            "Pod %s - Namespace %s | Pod finished running, cleaning up ..." % (
            pod_metadata.name, pod_metadata.namespace
        ))
        self._clean_up()


    def _execute_hooks(self):
        for hook in self.hooks:
            hook(pod=self.pod)

    
    def _clean_up(self):
        try:
            self.launcher.delete_pod(self.pod)
        except ResourceObjectNotFoundException as e:
            pass
        
        self.logger.info("Cleaned up! Everything done aweeeeeesomely")
