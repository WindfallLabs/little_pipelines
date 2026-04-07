"""

"""

import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .result import CacheResult
from .serialize import CacheSerializer, DefaultSerializer, StrSerializer
from little_pipelines import util


_DDL = """
CREATE TABLE IF NOT EXISTS cache (
    name TEXT PRIMARY KEY,
    data BLOB,
    dtype TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    extra TEXT
);
CREATE INDEX IF NOT EXISTS idx_cache_last_updated ON cache (last_updated)
    WHERE last_updated IS NOT NULL;
"""


class Cache():
    def __init__(self, path: str|Path):
        """A cache (SQLite database) to store results."""
        self.path = path
        if isinstance(path, str):
            self.path = Path(path)
        
        # Serialization rules
        self._serializers = {}
        self._serializers["default"] = DefaultSerializer()  # Pickle
        self._serializers[str(str)] = StrSerializer()

        # Database
        self._conn: Optional[sqlite3.Connection] = None
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def _default_serializer(self):
        return self._serializers["default"]

    def _open(self) -> None:
        """Open (create if needed) the database."""
        self._conn = sqlite3.connect(
            self.path,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._conn.executescript(_DDL)
        self._conn.commit()
        return

    def _close(self) -> None:
        """Commit pending writes and close the connection."""
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None
        return

    def __enter__(self) -> "Cache":
        self._open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._close()
        return False

    def __del__(self) -> None:
        self._close()
        return

    def serializer(self, type_arg: type):
        """
        Decorator to register a CacheSerializer subclass.

        Args:
            type_arg: Type to associate with the serializer.

        Returns:
            A decorator function that registers the serializer class.

        Example:
            ```
            import sys

            @cache.serializer
            class StrSerializer(CacheSerializer):
                def dumps(self, data: str, encoding: Optional[str] = None) -> bytes:
                    '''Defines how strings get written to the cache.'''
                    if not encoding:
                        encoding = sys.getdefaultencoding()
                    return data.encode(encoding)

                def loads(self, data: bytes, encoding: Optional[str] = None) -> str:
                    '''Defines how strings get read from the cache.'''
                    if not encoding:
                        encoding = sys.getdefaultencoding()
                    return data.decode(encoding)
            ```
        """
        def decorator(serializer_class: type[CacheSerializer]) -> type[CacheSerializer]:
            # Determine the type key
            if type_arg is not None:
                type_key = str(type_arg)
            else:
                raise ValueError("`@Cache().serializer(type)` decorator requires a type")

            # Store an instance of the serializer
            self._serializers[type_key] = serializer_class()

            return serializer_class
        
        return decorator

    def keys(self):
        """The names of cached data."""
        with self:
            rows = self._conn.execute("SELECT name FROM cache").fetchall()
        return [i[0] for i in rows]

    def get(self, name: str):
        """Get the cached data and metadata as a CacheResult object."""
        # Get from database
        if name in self.keys():
            result: CacheResult = CacheResult.read(name, self)
            return result
        raise KeyError(f"{name} not found in cache")

    def set(
        self,
        name: str,
        data: Any,
        extra: Optional[dataclass] = None,
    ) -> None:
        """Caches data and metadata to SQLite."""
        last_updated: dt.datetime = dt.datetime.now()
        dtype: str = str(type(data))
        # Write to the database
        _ = CacheResult(name, data, dtype, last_updated, extra).write(self)
        return

    def clear(self, name: Optional[str] = None):
        """Clears a record from the cache, or rebuilds the cache table."""
        with self:
            if name:
                try:
                    _ = self._conn.execute("DELETE FROM cache WHERE name = ?", (name,)).fetchall()
                except KeyError:
                    return False
            else:
                _ = self._conn.execute("DROP TABLE cache;").fetchall()
                _ = self._conn.executescript(_DDL).fetchall()
                _ = self._conn.execute("VACUUM;").fetchall()
        return True


default_cache = Cache(
    util.DEFAULT_CACHE_FILE
)
