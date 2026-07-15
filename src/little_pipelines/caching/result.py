"""
Wrapper class for cached data (and metadata/extras).


The `extra` attribute must be a (pickled) dataclass or None.
"""

import datetime as dt
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from hashlib import sha256
from typing import Any, Optional, TYPE_CHECKING

from .serialize import CacheSerializer, Serializer

if TYPE_CHECKING:
    from .cache import Cache


_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"


@dataclass(eq=True)
class Result:
    name: str
    task: str
    data: Any
    dtype: Optional[str] = None
    last_updated: Optional[dt.datetime] = None
    expiry: Optional[dt.datetime] = None
    extra: Optional[dict] = None

    _datetime_format = _DATETIME_FMT

    def __post_init__(self):
        if self.dtype is None:
            self.dtype = str(type(self.data))
        if self.last_updated is None:
            self.last_updated = dt.datetime.now()

    def as_row(self, serializer: Serializer):
        """Return the Result as an SQLite-insertable row."""
        serialized_data: bytes = serializer.dumps(self.data)
        last_updated: str = self.last_updated.strftime(self._datetime_format)
        expiry: str = self.expiry.strftime(self._datetime_format) if self.expiry else None
        extra: str = json.dumps(self.extra)
        row = (
            self.name,
            self.task,
            self.dtype,
            last_updated,
            expiry,
            serialized_data,
            extra,
        )
        return row

    @classmethod
    def from_row(cls, row, cache: "Cache") -> "Result":
        """Deserialize data from an SQLite row."""
        serializer: Serializer = cache.get_serializer(row["dtype"])
        data: Any = serializer.loads(row["data"])
        last_updated: dt.datetime = dt.datetime.strptime(row["last_updated"], cls._datetime_format)
        expiry: dt.datetime = dt.datetime.strptime(row["expiry"], cls._datetime_format) if row["expiry"] else None
        extra: dict = json.loads(row["extra"])
        result = cls(
            row["name"],
            row["task"],
            data,
            row["dtype"],
            last_updated,
            expiry,
            extra,
        )
        return result


# ================================================================================================

@dataclass
class CacheResult:
    name: str
    data: bytes
    dtype: Optional[str] = None
    last_updated: Optional[dt.datetime] = None
    extra: Optional[dataclass] = None

    @staticmethod
    def _select(conn: sqlite3.Connection, name: str) -> tuple[
        str, dt.datetime, Optional[str], str, bytes
    ]:
        """Selects data from the database."""
        row = conn.execute(
            "SELECT * FROM cache WHERE name = ?", (name,)
        ).fetchone()
        return row

    @staticmethod
    def _insert(
        conn: sqlite3.Connection,
        name: str,
        data: bytes,
        dtype: str,
        last_updated: dt.datetime,
        extra: Optional[bytes] = None,
    ) -> None:
        """Updates or inserts data into the database."""
        if not isinstance(data, bytes):
            raise TypeError(f"'data' must be converted to bytes. Currently {dtype}")
        if extra is not None and not isinstance(extra, str):
            raise TypeError(f"'extra' must be converted to str.")
        # Insert
        conn.execute(
            """
            INSERT OR REPLACE INTO cache (name, data, dtype, last_updated, extra)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                name,
                data,
                dtype,
                last_updated,
                extra,
            )
        )
        return

    @classmethod
    def read(cls, name: str, cache: "Cache") -> "CacheResult":
        """Reads the result (and metadata) from the database."""
        # Unpack database row
        with cache:
            name, raw_data, dtype, last_updated, extra = cls._select(cache._conn, name)
        last_updated = dt.datetime.strptime(last_updated, _DATETIME_FMT)
        if extra:
            extra = json.loads(extra)
        # Check for a custom serializer, else use default pickle
        # Leave bytes as-is if found
        if dtype == "<class 'bytes'>":
            data = raw_data
        else:
            serializer: CacheSerializer = cache._serializers.get(dtype, cache._default_serializer)
            data: Any = serializer.loads(raw_data)

        result = cls(name, data, dtype, last_updated, extra)
        return result

    def write(self, cache: "Cache") -> None:
        """Writes the result (and metadata) to the database."""
        # Check for a custom serializer, else use default pickle
        if self.dtype == "<class 'bytes'>":
            data: bytes = self.data
        else:
            serializer: CacheSerializer = cache._serializers.get(self.dtype, cache._default_serializer)
            data: bytes = serializer.dumps(self.data)
        # Last updated as string
        last_updated = self.last_updated.strftime(_DATETIME_FMT)
        # Dump extra (dataclass)
        if self.extra is not None and not is_dataclass(self.extra):
            raise TypeError(f"Extra must be a dataclass, not {type(self.extra)}")
        if not self.extra:
            extra = None
        else:
            extra = json.dumps(asdict(self.extra))

        # Write database rows
        with cache:
            self._insert(
                cache._conn,
                self.name,
                data,
                self.dtype,
                last_updated,  # converted to str
                extra,  # converted to JSON
            )

        return
