"""
Wrapper class for cached data (and metadata/extras).


The `extra` attribute must be a (pickled) dataclass or None.
"""

import datetime as dt
import json
import sqlite3
from hashlib import sha256
from typing import Any, Callable, Optional, TYPE_CHECKING

from .serialize import CacheSerializer, Serializer

if TYPE_CHECKING:
    from .cache import Cache


_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"


class Result:
    """An artifact (of Task) representing cached data."""
    def __init__(
        self,
        name: str,
        data: Any,
        task_name: str,
        dtype: Optional[str] = None,
        last_updated: Optional[dt.datetime] = None,
        expiry: Optional[dt.datetime] = None,
        extra: Optional[dict] = None,
    ):
        self.name = name
        self.task_name = task_name
        self.data = data
        self.dtype = dtype if dtype is not None else str(type(data))
        self.last_updated = last_updated if last_updated is not None else dt.datetime.now()
        self.expiry = expiry
        self.extra = extra
        self._datetime_format = _DATETIME_FMT

    def __eq__(self, other):
        if not isinstance(other, Result):
            return NotImplemented
        return (
            self.name == other.name
            and self.task_name == other.task_name
            and self.data == other.data
            and self.dtype == other.dtype
            and self.last_updated == other.last_updated
            and self.expiry == other.expiry
            and self.extra == other.extra
        )

    def __repr__(self):
        _cls = str(self.dtype).replace("<class '", "").replace("'>", "")
        #return f"<Result '{self.name}' ({self.dtype})>"
        return f"<Result '{self.name}' ({_cls})>"
