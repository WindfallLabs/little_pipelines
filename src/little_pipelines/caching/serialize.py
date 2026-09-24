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
    def dumps(self, data: Any) -> bytes:
        """
        Define how to pickle data.
        """
        ...

    @classmethod
    @abstractmethod
    def loads(self, data: bytes) -> Any:
        """
        Define how to unpickle data.
        """
        ...


class DefaultSerializer(Serializer):
    """Defines the default caching (using pickle)."""
    def dumps(self, data: Any) -> bytes:
        """Pickle data."""
        return pickle.dumps(data)

    def loads(self, data: bytes) -> Any:
        """Unpickle data."""
        return pickle.loads(data)


class StrSerializer(Serializer):
    def dumps(self, data: str) -> bytes:
        """Defines how strings get written to the cache."""
        encoding = sys.getdefaultencoding()
        return data.encode(encoding)

    def loads(self, data: bytes) -> str:
        """Defines how strings get read from the cache."""
        encoding = sys.getdefaultencoding()
        return data.decode(encoding)


__all__ = [
    "Serializer",
    "DefaultSerializer",
    "StrSerializer"
]
