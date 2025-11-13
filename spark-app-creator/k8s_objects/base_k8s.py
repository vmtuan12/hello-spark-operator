from typing import Any

class BaseKubernetesObject:
    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """

    openapi_types: dict[str, str] = dict()
    attribute_map: dict[str, str] = dict()

    def to_dict(self) -> dict[str, Any]:
        result = dict()

        for attr, attr_type in self.openapi_types.items():
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(
                    map(lambda x: x.to_dict() if hasattr(x, "to_dict") else x, value)
                )

            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()

            elif isinstance(value, dict):
                result[attr] = dict(
                    map(lambda x: (x[0], x[1].to_dict()) if hasattr(x[1], "to_dict") else x, value.items())
                )

            else:
                result[attr] = value

        return result
