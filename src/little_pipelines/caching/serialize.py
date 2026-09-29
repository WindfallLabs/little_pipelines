"""
Serialize - Define how data gets serialized for caching.
"""

import pickle
import sys
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """
    Abstract Base Class for data serializers
    """
    @classmethod
    @abstractmethod
    def dumps(self, value: Any) -> bytes:
        """
        Define how to pickle a value of data.
        """
        ...

    @classmethod
    @abstractmethod
    def loads(self, value: bytes) -> Any:
        """
        Define how to unpickle a value of data.
        """
        ...


class DefaultSerializer(Serializer):
    """Defines the default caching (using pickle)."""
    def dumps(self, value: Any) -> bytes:
        """Pickle value."""
        return pickle.dumps(value)

    def loads(self, value: bytes) -> Any:
        """Unpickle value."""
        return pickle.loads(value)


class StrSerializer(Serializer):
    def dumps(self, value: str) -> bytes:
        """Defines how strings get written to the cache."""
        encoding = sys.getdefaultencoding()
        return value.encode(encoding)

    def loads(self, value: bytes) -> str:
        """Defines how strings get read from the cache."""
        encoding = sys.getdefaultencoding()
        return value.decode(encoding)


__all__ = [
    "Serializer",
    "DefaultSerializer",
    "StrSerializer"
]
