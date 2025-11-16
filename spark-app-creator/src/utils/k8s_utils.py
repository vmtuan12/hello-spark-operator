from kubernetes.client.models import V1ObjectMeta, V1Pod, V1Status
from enum import Enum

class PodStatusPhaseEnum(Enum):
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"

class PodEventTypeEnum(Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETE = "DELETE"
    ERROR = "ERROR"

def get_pod_status_phase(pod: V1Pod):
    status: V1Status = pod.status
    return status.phase

