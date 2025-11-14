from __future__ import annotations

from enum import Enum

from k8s_objects import BaseKubernetesObject
from kubernetes.client.models import (V1Affinity, V1Container, V1EnvFromSource,
                                      V1EnvVar, V1HostAlias, V1ObjectMeta,
                                      V1PodDNSConfig, V1PodSecurityContext,
                                      V1SecurityContext, V1Toleration,
                                      V1Volume, V1VolumeMount, V1Lifecycle, V1IngressTLS)


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
SecretType = str

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
        "batch_scheduler_options": "BatchSchedulerConfiguration",
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
        batch_scheduler_options: BatchSchedulerConfiguration = None,
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


class SparkPodSpec(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "cores": "int",
        "core_limit": "str",
        "memory": "str",
        "memory_overhead": "str",
        "image": "str",
        "config_maps": "list[NamePath]",
        "secrets": "list[SecretInfo]",
        "env": "list[V1EnvVar]",
        "env_from": "list[V1EnvFromSource]",
        "labels": "dict[str, str]",
        "annotations": "dict[str, str]",
        "volume_mounts": "list[V1VolumeMount]",
        "affinity": "V1Affinity",
        "tolerations": "list[V1Toleration]",
        "pod_security_context": "V1PodSecurityContext",
        "security_context": "V1SecurityContext",
        "scheduler_name": "str",
        "sidecars": "V1Container",
        "init_containers": "V1Container",
        "host_network": "bool",
        "node_selector": "dict[str, str]",
        "dns_config": "V1PodDNSConfig",
        "termination_grace_period_seconds": "int",
        "service_account": "str",
        "host_aliases": "V1HostAlias",
        "share_process_namespace": "bool",
        "java_options": "str",
        "core_request": "str",
    }
    attribute_map: dict[str, str] = {
        "cores": "cores",
        "core_limit": "coreLimit",
        "memory": "memory",
        "memory_overhead": "memoryOverhead",
        "gpu": "gpu",
        "image": "image",
        "config_maps": "configMaps",
        "secrets": "secrets",
        "env": "env",
        "env_vars": "envVars",
        "env_from": "envFrom",
        "env_secret_key_refs": "envSecretKeyRefs",
        "labels": "labels",
        "annotations": "annotations",
        "volume_mounts": "volumeMounts",
        "affinity": "affinity",
        "tolerations": "tolerations",
        "pod_security_context": "podSecurityContext",
        "security_context": "securityContext",
        "scheduler_name": "schedulerName",
        "sidecars": "sidecars",
        "init_containers": "initContainers",
        "host_network": "hostNetwork",
        "node_selector": "nodeSelector",
        "dns_config": "dnsConfig",
        "termination_grace_period_seconds": "terminationGracePeriodSeconds",
        "service_account": "serviceAccount",
        "host_aliases": "hostAliases",
        "share_process_namespace": "shareProcessNamespace",
        "java_options": "javaOptions",
        "core_request": "coreRequest",
    }

    def __init__(self,
                cores: int = None,
                core_limit: str = None,
                core_request: str = None,
                java_options: str = None,
                memory: str = None,
                memory_overhead: str = None,
                image: str = None,
                config_maps: list[NamePath] = None,
                secrets: list[SecretInfo] = None,
                env: list[V1EnvVar] = None,
                env_from: list[V1EnvFromSource] = None,
                labels: dict[str, str] = None,
                annotations: dict[str, str] = None,
                volume_mounts: list[V1VolumeMount] = None,
                affinity: V1Affinity = None,
                tolerations: list[V1Toleration] = None,
                pod_security_context: V1PodSecurityContext = None,
                security_context: V1SecurityContext = None,
                scheduler_name: str = None,
                sidecars: V1Container = None,
                init_containers: V1Container = None,
                host_network: bool = None,
                node_selector: dict[str, str] = None,
                dns_config: V1PodDNSConfig = None,
                termination_grace_period_seconds: int = None,
                service_account: str = None,
                host_aliases: V1HostAlias = None,
                share_process_namespace: bool = None,
    ):
        self.cores = cores
        self.core_limit = core_limit
        self.memory = memory
        self.memory_overhead = memory_overhead
        self.image = image
        self.config_maps = config_maps
        self.secrets = secrets
        self.env = env
        self.env_from = env_from
        self.labels = labels
        self.annotations = annotations
        self.volume_mounts = volume_mounts
        self.affinity = affinity
        self.tolerations = tolerations
        self.pod_security_context = pod_security_context
        self.security_context = security_context
        self.scheduler_name = scheduler_name
        self.sidecars = sidecars
        self.init_containers = init_containers
        self.host_network = host_network
        self.node_selector = node_selector
        self.dns_config = dns_config
        self.termination_grace_period_seconds = termination_grace_period_seconds
        self.service_account = service_account
        self.host_aliases = host_aliases
        self.share_process_namespace = share_process_namespace
        self.java_options = java_options
        self.core_request = core_request


class NameKey(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "name": "str",
        "key": "str"
    }

    attribute_map: dict[str, str] = {
        "name": "name",
        "key": "key"
    }

    def __init__(self,
                 name: str = None,
                 key: str = None):
        """NameKey represents the name and key of a SecretKeyRef."""
        self.name = name
        self.key = key


class SecretInfo(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "name": "str",
        "path": "str",
        "secret_type": "SecretType"
    }

    attribute_map: dict[str, str] = {
        "name": "name",
        "path": "path",
        "secret_type": "secretType"
    }

    def __init__(self,
                 name: str = None,
                 path: str = None,
                 secret_type: SecretType = None):
        """SecretInfo captures information of a secret."""
        self.name = name
        self.path = path
        self.secret_type = secret_type

class NamePath(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "name": "str",
        "path": "str"
    }

    attribute_map: dict[str, str] = {
        "name": "name",
        "path": "path"
    }

    def __init__(self,
                 name: str = None,
                 path: str = None):
        """NamePath is a pair of a name and a path to which the named objects should be mounted to."""
        self.name = name
        self.path = path

class SparkDriverSpec(SparkPodSpec):
    openapi_types: dict[str, str] = {
        **SparkPodSpec.openapi_types,
        "pod_name": "str",
        "lifecycle": "V1Lifecycle",
        "kubernetes_master": "str",
        "service_annotations": "dict[str, str]",
    }

    attribute_map: dict[str, str] = {
        **SparkPodSpec.attribute_map,
        "pod_name": "podName",
        "lifecycle": "lifecycle",
        "kubernetes_master": "kubernetesMaster",
        "service_annotations": "serviceAnnotations",
    }

    def __init__(self, 
                 pod_name: str,
                 lifecycle: V1Lifecycle,
                 kubernetes_master: str,
                 service_annotations: dict[str, str],
                 **kwargs):
        super().__init__(**kwargs)
        self.pod_name = pod_name
        self.lifecycle = lifecycle
        self.kubernetes_master = kubernetes_master
        self.service_annotations = service_annotations


class SparkExecutorSpec(SparkPodSpec):
    openapi_types: dict[str, str] = {
        **SparkPodSpec.openapi_types,
        "instances": "int",
        "delete_on_termination": "bool",
    }

    attribute_map: dict[str, str] = {
        **SparkPodSpec.attribute_map,
        "instances": "instances",
        "delete_on_termination": "deleteOnTermination",
    }

    def __init__(self, 
                 instances: int,
                 delete_on_termination: bool,
                 **kwargs):
        super().__init__(**kwargs)
        self.instances = instances
        self.delete_on_termination = delete_on_termination


class SparkDependencies(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "jars": "list[str]",
        "files": "list[str]",
        "py_files": "list[str]"
    }

    attribute_map: dict[str, str] = {
        "jars": "jars",
        "files": "files",
        "py_files": "pyFiles"
    }

    def __init__(self,
                 jars: list[str] = None,
                 files: list[str] = None,
                 py_files: list[str] = None):
        """Dependencies specifies all possible types of dependencies of a Spark application."""
        self.jars = jars
        self.files = files
        self.py_files = py_files


RestartPolicyType = str

class RestartPolicyTypeEnum(str, Enum):
    NEVER = "Never"
    ON_FAILURE = "OnFailure"
    ALWAYS = "Always"

class RestartPolicy(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "type": "RestartPolicyType",
        "on_submission_failure_retries": "int",
        "on_failure_retries": "int",
        "on_submission_failure_retry_interval": "int",
        "on_failure_retry_interval": "int"
    }

    attribute_map = {
        "type": "type",
        "on_submission_failure_retries": "onSubmissionFailureRetries",
        "on_failure_retries": "onFailureRetries",
        "on_submission_failure_retry_interval": "onSubmissionFailureRetryInterval",
        "on_failure_retry_interval": "onFailureRetryInterval"
    }

    def __init__(self,
                 type: RestartPolicyType = None,
                 on_submission_failure_retries: int = None,
                 on_failure_retries: int = None,
                 on_submission_failure_retry_interval: int = None,
                 on_failure_retry_interval: int = None):
        """
        RestartPolicy is the policy of if and in which conditions the controller should restart a terminated application.
        This completely defines actions to be taken on any kind of Failures during an application run.
        """
        self.type = type
        self.on_submission_failure_retries = on_submission_failure_retries
        self.on_failure_retries = on_failure_retries
        self.on_submission_failure_retry_interval = on_submission_failure_retry_interval
        self.on_failure_retry_interval = on_failure_retry_interval


class SparkMonitoringSpec(BaseKubernetesObject):
    openapi_types = {
        "expose_driver_metrics": "bool",
        "expose_executor_metrics": "bool",
        "metrics_properties": "str",
        "metrics_properties_file": "str",
        "prometheus": "PrometheusSpec"
    }

    attribute_map = {
        "expose_driver_metrics": "exposeDriverMetrics",
        "expose_executor_metrics": "exposeExecutorMetrics",
        "metrics_properties": "metricsProperties",
        "metrics_properties_file": "metricsPropertiesFile",
        "prometheus": "prometheus"
    }

    def __init__(self,
                 expose_driver_metrics: bool = None,
                 expose_executor_metrics: bool = None,
                 metrics_properties: str = None,
                 metrics_properties_file: str = None,
                 prometheus: PrometheusSpec = None):
        """MonitoringSpec defines the monitoring specification."""
        self.expose_driver_metrics = expose_driver_metrics
        self.expose_executor_metrics = expose_executor_metrics
        self.metrics_properties = metrics_properties
        self.metrics_properties_file = metrics_properties_file
        self.prometheus = prometheus

class PrometheusSpec(BaseKubernetesObject):
    openapi_types = {
        "jmx_exporter_jar": "str",
        "port": "int",
        "port_name": "int",
        "config_file": "str",
        "configuration": "str"
    }

    attribute_map = {
        "jmx_exporter_jar": "jmxExporterJar",
        "port": "port",
        "port_name": "portName",
        "config_file": "configFile",
        "configuration": "configuration"
    }

    def __init__(self,
                 jmx_exporter_jar: str = None,
                 port: int = None,
                 port_name: str = None,
                 config_file: str = None,
                 configuration: str = None):
        """PrometheusSpec defines the Prometheus specification when
        Prometheus is to be used for collecting and exposing metrics."""
        self.jmx_exporter_jar = jmx_exporter_jar
        self.port = port
        self.port_name = port_name
        self.config_file = config_file
        self.configuration = configuration



class BatchSchedulerConfiguration(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "queue": "str",
        "priority_class_name": "str"
    }

    attribute_map: dict[str, str] = {
        "queue": "queue",
        "priority_class_name": "priorityClassName"
    }

    def __init__(self,
                 queue: str = None,
                 priority_class_name: str = None):
        """BatchSchedulerConfiguration used to configure how to batch scheduling Spark Application."""
        self.queue = queue
        self.priority_class_name = priority_class_name


class SparkUIOptions(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "service_port": "int",
        "service_port_name": "str",
        "service_type": "str",
        "service_annotations": "dict[str, str]",
        "ingress_annotations": "dict[str, str]",
        "ingress_tls": "list[V1IngressTLS]",
    }

    attribute_map: dict[str, str] = {
        "service_port": "servicePort",
        "service_port_name": "servicePortName",
        "service_type": "serviceType",
        "service_annotations": "serviceAnnotations",
        "ingress_annotations": "ingressAnnotations",
        "ingress_tls": "ingressTLS",
    }

    def __init__(self,
                 service_port: int = None,
                 service_port_name: str = None,
                 service_type: str = None,
                 service_annotations: dict[str, str] = None,
                 ingress_annotations: dict[str, str] = None,
                 ingress_tls: list[V1IngressTLS] = None):
        """SparkUIConfiguration is for driver UI specific configuration parameters."""
        self.service_port = service_port
        self.service_port_name = service_port_name
        self.service_type = service_type
        self.service_annotations = service_annotations
        self.ingress_annotations = ingress_annotations
        self.ingress_tls = ingress_tls


class DynamicAllocation(BaseKubernetesObject):
    openapi_types: dict[str, str] = {
        "enabled": "bool",
        "initial_executors": "int",
        "min_executors": "int",
        "max_executors": "int",
        "shuffle_tracking_timeout": "int"
    }

    attribute_map: dict[str, str] = {
        "enabled": "enabled",
        "initial_executors": "initialExecutors",
        "min_executors": "minExecutors",
        "max_executors": "maxExecutors",
        "shuffle_tracking_timeout": "shuffleTrackingTimeout"
    }

    def __init__(self,
                 enabled: bool = None,
                 initial_executors: int = None,
                 min_executors: int = None,
                 max_executors: int = None,
                 shuffle_tracking_timeout: int = None):
        """DynamicAllocation contains configuration options for dynamic allocation."""
        self.enabled = enabled
        self.initial_executors = initial_executors
        self.min_executors = min_executors
        self.max_executors = max_executors
        self.shuffle_tracking_timeout = shuffle_tracking_timeout



