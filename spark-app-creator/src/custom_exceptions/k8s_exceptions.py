class PodFailedException(Exception):
    pass

class SparkAppFailedException(Exception):
    pass

class SparkAppSubmissionFailedException(Exception):
    pass

class PermissionDeniedException(Exception):
    pass


class ResourceObjectNotFoundException(Exception):
    def __init__(self, resource_type: str, message: str | None = None) -> None:
        self.resource_type = resource_type
        super().__init__(message)

