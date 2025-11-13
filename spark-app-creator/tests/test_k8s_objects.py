from k8s_objects import BaseKubernetesObject

def test_k8s_object_to_dict():
    class Dz(BaseKubernetesObject):
        openapi_types = {
            "xxx": "str",
        }

        def __init__(self) -> None:
            self.xxx = "tuan dz"

    class A(BaseKubernetesObject):
        openapi_types = {
            "a": "str",
            "b": "list",
            "c": "Dz"
        }

        def __init__(self) -> None:
            self.a = "123"
            self.b = [1, 2, 3]
            self.c = Dz()

    assert A().to_dict() == {"a": "123", "b": [1, 2, 3], "c": {"xxx": "tuan dz"}}
