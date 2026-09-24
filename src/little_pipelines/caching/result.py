"""
Result - Data-Cache interop-object.
"""

import datetime as dt
from typing import Any, Optional, TYPE_CHECKING

from .serialize import Serializer

if TYPE_CHECKING:
    from .cache import Cache


_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"


class Result:
    """
    A serializable runtime artifact produced by a Task and persisted by Cache.
    """
    def __init__(
        self,
        name: str,
        data: Any,
        task_name: str,
        dtype: Optional[str] = None,
        last_updated: Optional[dt.datetime] = None,
        expiry: Optional[dt.datetime] = None,  # TODO: WIP
        extra: Optional[dict] = None,
    ):
        """
        Initialize a Result

        Args:
            name (str): The name and primary identifier of the Result.
            data (Any): The data to store or that is being retrieved.
            task_name (str): The name of the originating task.
            dtype (str): The name of the datatype.
            last_updated (dt.datetime): The creation or update date.
            expiry (WIP): (dt.datetime): The date to expire the data by.
            extra (dict): Extra information to attach to the cached Result.
        """
        self.name = name
        self.task_name = task_name
        self.data = data
        self.dtype = dtype if dtype is not None else str(type(data))
        self.last_updated = last_updated if last_updated is not None else dt.datetime.now()
        self.expiry = expiry
        self.extra = extra
        self._datetime_format = _DATETIME_FMT

    def __eq__(self, other) -> bool:
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
        return f"<Result '{self.name}' ({_cls})>"


__all__ = ["Result"]

