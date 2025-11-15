import kubernetes
from functools import cached_property

import logging

class BaseClient():
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
        kubernetes.config.load_incluster_config()
        return kubernetes.client.ApiClient()

