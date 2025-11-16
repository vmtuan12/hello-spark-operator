import logging
from functools import cached_property
from typing import Generator, Iterator

import kubernetes
from kubernetes.client.models import V1ObjectMeta, V1Pod, V1WatchEvent


class BaseLauncher():
    def __init__(self, api_client: kubernetes.client.ApiClient) -> None:
        self.core_v1_api = kubernetes.client.CoreV1Api(api_client=api_client)

    @property
    def logger(self) -> logging.Logger:
        return self._setup_logger()


    def _setup_logger(self) -> logging.Logger:
        """
        Override this function if you need to customize the logger
        """
        logger_name = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return logging.getLogger(logger_name)


    def _read_pod_log(self, pod: V1Pod, tail_lines: int = 10) -> Iterator[bytes]:
        pod_metadata: V1ObjectMeta = pod.metadata
        pod_namespace = pod_metadata.namespace
        pod_name = pod_metadata.name

        kwargs = dict()
        if tail_lines:
            kwargs["tail_lines"] = tail_lines

        return self.core_v1_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=pod_namespace,
                follow=True,
                _preload_content=False,
                **kwargs
        )

