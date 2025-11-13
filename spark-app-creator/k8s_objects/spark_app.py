from k8s_objects import BaseKubernetesObject
from enum import Enum
from kubernetes.client.models import V1ObjectMeta, V1Volume
from __future__ import annotations

class SparkApp(BaseKubernetesObject):
    """
    Reference: https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md#sparkapplication
    """

    openapi_types: dict[str, str] = {
        "api_version": "str",
        "kind": "str",
        "metadata": "V1ObjectMeta",
        "spec": "SparkAppSpec",
        "status": "SparkAppStatus",
    }
    attribute_map: dict[str, str] = {
        "api_version": "apiVersion",
        "kind": "kind",
        "metadata": "metadata",
        "spec": "spec",
        "status": "status",
    }

    def __init__(
        self,
        api_version: str,
        kind: str,
        metadata: V1ObjectMeta,
        spec: SparkAppSpec,
        status: SparkAppStatus,
    ) -> None:
        self.api_version = api_version
        self.kind = kind
        self.metadata = metadata if metadata is not None else V1ObjectMeta()
        self.spec = spec if spec is not None else SparkAppSpec()
        self.status = status if status is not None else SparkAppStatus()


SparkApplicationType = str
SparkDeployMode = str

class SparkApplicationTypeEnum(Enum):
    JAVA = "Java"
    PYTHON = "Python"
    SCALA = "Scala"
    R = "R"

class SparkDeployModeEnum(Enum):
    CLIENT = "client"
    CLUSTER = "cluster"
    IN_CLUSTER_CLIENT = "in-cluster-client"


class SparkAppSpec(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "type": "SparkApplicationType",
        "spark_version": "str",
        "mode": "SparkDeployMode",
        "proxy_user": "str",
        "image": "str",
        "image_pull_policy": "str",
        "image_pull_secrets": "list[str]",
        "main_class": "str",
        "main_application_file": "str",
        "arguments": "list[str]",
        "spark_conf": "dict[str, str]",
        "hadoop_conf": "dict[str, str]",
        "spark_config_map": "str",
        "hadoop_config_map": "str",
        "volumes": "list[V1Volume]",
        "driver": "SparkDriverSpec",
        "executor": "SparkExecutorSpec",
        "deps": "SparkDependencies",
        "restart_policy": "RestartPolicy",
        "node_selector": "dict[str, str]",
        "failure_retries": "int",
        "retry_interval": "int",
        "python_version": "str",
        "memory_overhead_factor": "str",
        "monitoring": "SparkMonitoringSpec",
        "batch_scheduler": "str",
        "time_to_live_seconds": "int",
        "batch_scheduler_options": "BatchSchedulerOptions",
        "spark_ui_options": "SparkUIOptions",
        "dynamic_allocation": "DynamicAllocation",
    }
    attribute_map: dict[str, str] = {
        "type": "type",
        "spark_version": "sparkVersion",
        "mode": "mode",
        "proxy_user": "proxyUser",
        "image": "image",
        "image_pull_policy": "imagePullPolicy",
        "image_pull_secrets": "imagePullSecrets",
        "main_class": "mainClass",
        "main_application_file": "mainApplicationFile",
        "arguments": "arguments",
        "spark_conf": "sparkConf",
        "hadoop_conf": "hadoopConf",
        "spark_config_map": "sparkConfigMap",
        "hadoop_config_map": "hadoopConfigMap",
        "volumes": "volumes",
        "driver": "driver",
        "executor": "executor",
        "deps": "deps",
        "restart_policy": "restartPolicy",
        "node_selector": "nodeSelector",
        "failure_retries": "failureRetries",
        "retry_interval": "retryInterval",
        "python_version": "pythonVersion",
        "memory_overhead_factor": "memoryOverheadFactor",
        "monitoring": "monitoring",
        "batch_scheduler": "batchScheduler",
        "time_to_live_seconds": "timeToLiveSeconds",
        "batch_scheduler_options": "batchSchedulerOptions",
        "spark_ui_options": "sparkUIOptions",
        "dynamic_allocation": "dynamicAllocation",
    }
    def __init__(
        self,
        spark_version: str,
        image: str,
        main_application_file: str,
        driver: SparkDriverSpec,
        executor: SparkExecutorSpec,
        type: SparkApplicationType = SparkApplicationTypeEnum.PYTHON.value,
        mode: SparkDeployMode = SparkDeployModeEnum.CLUSTER.value,
        spark_conf: dict[str, str] = None,
        image_pull_policy: str = None,
        proxy_user: str = None,
        image_pull_secrets: list[str] = None,
        main_class: str = None,
        arguments: list[str] = None,
        hadoop_conf: dict[str, str] = None,
        spark_config_map: str = None,
        hadoop_config_map: str = None,
        volumes: list[V1Volume] = None,
        deps: SparkDependencies = None,
        restart_policy: RestartPolicy = None,
        node_selector: dict[str, str] = None,
        failure_retries: int = None,
        retry_interval: int = None,
        python_version: str = None,
        memory_overhead_factor: str = None,
        monitoring: SparkMonitoringSpec = None,
        batch_scheduler: str = None,
        time_to_live_seconds: int = None,
        batch_scheduler_options: BatchSchedulerOptions = None,
        spark_ui_options: SparkUIOptions = None,
        dynamic_allocation: DynamicAllocation = None,
    ) -> None:
        self.spark_version = spark_version
        self.image = image
        self.main_application_file = main_application_file
        self.driver = driver
        self.executor = executor
        self.type = type
        self.mode = mode
        self.spark_conf = spark_conf
        self.image_pull_policy = image_pull_policy
        self.proxy_user = proxy_user
        self.image_pull_secrets = image_pull_secrets
        self.main_class = main_class
        self.arguments = arguments
        self.hadoop_conf = hadoop_conf
        self.spark_config_map = spark_config_map
        self.hadoop_config_map = hadoop_config_map
        self.volumes = volumes
        self.deps = deps
        self.restart_policy = restart_policy
        self.node_selector = node_selector
        self.failure_retries = failure_retries
        self.retry_interval = retry_interval
        self.python_version = python_version
        self.memory_overhead_factor = memory_overhead_factor
        self.monitoring = monitoring
        self.batch_scheduler = batch_scheduler
        self.time_to_live_seconds = time_to_live_seconds
        self.batch_scheduler_options = batch_scheduler_options
        self.spark_ui_options = spark_ui_options
        self.dynamic_allocation = dynamic_allocation


class SparkAppStatus(BaseKubernetesObject):
    pass


class SparkDriverSpec(BaseKubernetesObject):
    pass


class SparkExecutorSpec(BaseKubernetesObject):
    pass


class SparkDependencies(BaseKubernetesObject):
    pass


class RestartPolicy(BaseKubernetesObject):
    pass


class SparkMonitoringSpec(BaseKubernetesObject):
    pass


class BatchSchedulerOptions(BaseKubernetesObject):
    pass


class SparkUIOptions(BaseKubernetesObject):
    pass


class DynamicAllocation(BaseKubernetesObject):
    pass



