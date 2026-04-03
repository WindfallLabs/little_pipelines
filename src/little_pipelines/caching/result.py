"""
Wrapper class for cached data (and metadata).
"""

import datetime as dt
import sqlite3
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Optional, TYPE_CHECKING

from .rules import CacheRule

if TYPE_CHECKING:
    from .cache import Cache


@dataclass
class CacheResult:
    name: str
    data: bytes
    dtype: Optional[str] = None
    last_updated: Optional[dt.datetime] = None
    hash: Optional[str] = None

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
        hash_: Optional[str] = None
    ) -> None:
        """Updates or inserts data into the database."""
        if not isinstance(data, bytes):
            raise TypeError(f"Data must be converted to bytes. Currently {dtype}")
        # Hash if not provided
        if hash_ == "":
            hash_ = sha256(data).hexdigest()
        conn.execute(
            """
            INSERT OR REPLACE INTO cache (name, data, dtype, last_updated, hash)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                name,
                data,
                dtype,
                last_updated,
                hash_
            )
        )
        return

    @classmethod
    def read(cls, name: str, cache: "Cache") -> "CacheResult":
        """Reads the result (and metadata) from the database."""
        # Unpack database row
        with cache:
            name, raw_data, dtype, last_updated, hash_ = cls._select(cache._conn, name)
        # Check for a custom serializer, else use default pickle
        # Leave bytes as-is if found
        if dtype == "<class 'bytes'>":
            data = raw_data
        else:
            rule: CacheRule = cache._rules.get(dtype, cache._default_rule)
            data: Any = rule.loads(raw_data)

        result = cls(name, data, dtype, last_updated, hash_)
        return result

    def write(self, cache: "Cache") -> None:
        """Writes the result (and metadata) to the database."""
        # Check for a custom serializer, else use default pickle
        if self.dtype == "<class 'bytes'>":
            data: bytes = self.data
        else:
            rule: CacheRule = cache._rules.get(self.dtype, cache._default_rule)
            data: bytes = rule.dumps(self.data)

        # Write database rows
        with cache:
            self._insert(
                cache._conn,
                self.name,
                data,
                self.dtype,
                self.last_updated,
                self.hash
            )

        return
