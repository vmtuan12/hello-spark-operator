import logging
from functools import cached_property
from typing import Callable
import kubernetes
from abc import abstractmethod


class BaseClient():
    def __init__(self, hooks: list[Callable] = None, is_client_outside_cluster: bool = False, context: str = None) -> None:
        self.hooks = hooks
        self.is_client_outside_cluster = is_client_outside_cluster
        self.context = context

    @property
    def logger(self) -> logging.Logger:
        return self._setup_logger()


    @cached_property
    def api_client(self) -> kubernetes.client.ApiClient:
        return self._get_api_client()


    def _setup_logger(self) -> logging.Logger:
        """
        Override this function if you need to customize the logger
        """
        logger_name = f"{self.__class__.__module__}.{self.__class__.__name__}"
        return logging.getLogger(logger_name)


    def _get_api_client(self) -> kubernetes.client.ApiClient:
        """
        Override this function if you need to customize the api client
        """
        if self.is_client_outside_cluster:
            if self.context is None:
                raise ValueError("context cannot be None when is_client_outside_cluster == True")
            kubernetes.config.load_kube_config(context=self.context)

        else:
            kubernetes.config.load_incluster_config()

        return kubernetes.client.ApiClient()


    @abstractmethod
    def _execute_hooks(self):
        raise Exception("Must override method `_execute_hooks` in %s.%s" % (self.__class__.__module__, self.__class__.__name__))


    @abstractmethod
    def _clean_up(self):
        raise Exception("Must override method `_clean_up` in %s.%s" % (self.__class__.__module__, self.__class__.__name__))
