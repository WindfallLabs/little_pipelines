"""
Define how datatypes get serialized before caching.
"""

import pickle
import sys
from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    @classmethod
    @abstractmethod
    def dumps(self, data: Any) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def loads(self, data: bytes) -> Any:
        ...


CacheSerializer = Serializer


# class DefaultSerializer(CacheSerializer):
#     """Defines the default caching (using pickle)."""
#     def dumps(self, data: Any) -> bytes:
#         """Pickle data."""
#         return pickle.dumps(data)

#     def loads(self, data: bytes) -> Any:
#         """Unpickle data."""
#         return pickle.loads(data)


# class StrSerializer(CacheSerializer):
#     def dumps(self, data: str, encoding: Optional[str] = None) -> bytes:
#         """Defines how strings get written to the cache."""
#         if not encoding:
#             encoding = sys.getdefaultencoding()
#         return data.encode(encoding)

#     def loads(self, data: bytes, encoding: Optional[str] = None) -> str:
#         """Defines how strings get read from the cache."""
#         if not encoding:
#             encoding = sys.getdefaultencoding()
#         return data.decode(encoding)


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
