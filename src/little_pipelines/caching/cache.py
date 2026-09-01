"""

"""

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .result import Result
from .serialize import Serializer, DefaultSerializer, StrSerializer


_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"

_SETUP_DDL = """
CREATE TABLE IF NOT EXISTS cache (
    name TEXT PRIMARY KEY,
    task TEXT NOT NULL,
    dtype TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    expiry TEXT,
    data BLOB,
    extra TEXT
);
CREATE INDEX IF NOT EXISTS idx_cache_task ON cache (task);
CREATE INDEX IF NOT EXISTS idx_cache_last_updated ON cache (last_updated)
    WHERE last_updated IS NOT NULL;
"""


class Cache:
    def __init__(self, database_path: str|Path = ":memory:"):
        self._database_path = database_path
        self.database_path = database_path
        self._conn: Optional[sqlite3.Connection] = None
        self.is_uri = False

        self._setup_database()

        # Serialization and rules
        self._serializers = {}
        self._default_serializer = DefaultSerializer()  # Pickle
        self._serializers["default"] = self._default_serializer
        self._serializers[str(bytes)] = self._default_serializer
        self._serializers[str(str)] = StrSerializer()

    def _setup_database(self):
        if self._database_path in ("memory", ":memory:"):
            # Creates a shared in-memory database
            self.database_path = "file:cachedb?mode=memory"  # &cache=shared
            self.is_uri = True
        self._conn = sqlite3.connect(
            self.database_path,
            uri=self.is_uri,
            detect_types=sqlite3.PARSE_DECLTYPES,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row  # Tip: use dict(row) to extract
        self._conn.execute("PRAGMA journal_mode=WAL").fetchone()

        self._conn.executescript(_SETUP_DDL)
        self._conn.commit()

        return

    def get(
        self,
        result_name: Optional[str] = None,
        task_name: Optional[str] = None,
        return_raw_rows=False
    ) -> list[Result] | list[dict[str, Any]]:
        """Gets a list of Results from the cache."""
        if not result_name and not task_name:
            raise AttributeError("Empty dependency name")
        if not ((result_name or task_name) and not (result_name and task_name)):
            raise AttributeError("Either a result_name or task_name is required")
        
        # Allow * wildcards ('*' -> '%')
        result_name = result_name.replace("*", "%") if result_name else None
        task_name = task_name.replace("*", "%") if task_name else None
        # if "%" not in result_name and result_name not in self.keys():
        #     raise KeyError(f"{result_name} not found in cache")

        rows = (
            self._conn.execute(
                "SELECT * FROM cache WHERE name LIKE ? OR task LIKE ?", (result_name, task_name)
            )
            .fetchall()
        )

        if return_raw_rows:
            return [dict(r) for r in rows]

        results: list[Result] = []
        for row in rows:
            results.append(
                #Result.from_row(row, self)
                self._from_row(row)
            )
        assert isinstance(results, list)

        return results
        
    def put(self, result: Result, mode="UPSERT"):
        """Insert a Result into the cache."""
        if type(result) is not Result:
            raise TypeError("This method only accepts Result objects")
        serializer: Serializer = self.get_serializer(result.dtype)
        mode = mode.upper()
        if mode not in {'UPSERT', 'IGNORE', 'FAIL'}:
            raise KeyError("Mode must be one of 'UPSERT', 'IGNORE', or 'FAIL'")
        if mode == "UPSERT":
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache (name, task, dtype, last_updated, expiry, data, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                #result.as_row(serializer)
                self._to_row(result)
            )
        elif mode == "FAIL":
            try:
                self._conn.execute(
                    """
                    INSERT INTO cache (name, task, dtype, last_updated, expiry, data, extra)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    #result.as_row(serializer)
                    self._to_row(result)
                )
            except sqlite3.IntegrityError:
                raise sqlite3.IntegrityError(f"{result.name} already in cache")
        self._conn.commit()

        return

    def keys(self):
        """Return the names of data within the cache."""
        rows = self._conn.execute("SELECT name FROM cache").fetchall()
        return sorted([i[0] for i in rows])

    def clear(self, name: Optional[str] = None) -> bool:
        """Clears a record from the cache, or rebuilds the cache table."""
        if name:
            name = name.replace("*", "%")
            try:
                cur = self._conn.execute("DELETE FROM cache WHERE name LIKE ? OR task LIKE ?", (name, name))
                row_cnt = cur.rowcount
                _ = cur.fetchall()
                self._conn.commit()
                _ = self._conn.execute("VACUUM;").fetchall()
                if row_cnt > 0:
                    return True
                return False
            except Exception as e:
                return False
        else:
            _ = self._conn.execute("DROP TABLE cache;").fetchall()
            _ = self._setup_database()
            _ = self._conn.execute("VACUUM;").fetchall()
            return True


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

            @cache.serializer(str)
            class StrSerializer(Serializer):
                def dumps(self, data: str) -> bytes:
                    '''Defines how strings get written to the cache.'''
                    encoding = sys.getdefaultencoding()
                    return data.encode(encoding)

                def loads(self, data: bytes) -> str:
                    '''Defines how strings get read from the cache.'''
                    encoding = sys.getdefaultencoding()
                    return data.decode(encoding)
            ```
        """

        def decorator(serializer_class: type[Serializer]) -> type[Serializer]:
            # Determine the type key
            if type_arg is not None:
                type_key = str(type_arg)
            else:
                raise ValueError("`@Cache().serializer(type)` decorator requires a type")

            # Store an instance of the serializer
            self._serializers[type_key] = serializer_class()

            return serializer_class
        
        return decorator

    def get_serializer(self, dtype: str) -> Serializer:
        """Return a data serializer."""
        return self._serializers.get(dtype, self._default_serializer)


    def _to_row(self, result: Result) -> tuple:
        serializer = self.get_serializer(result.dtype)
        return (
            result.name,
            result.task_name,
            result.dtype,
            result.last_updated.strftime(_DATETIME_FMT),
            result.expiry.strftime(_DATETIME_FMT) if result.expiry else None,
            serializer.dumps(result.data),
            json.dumps(result.extra),
        )

    def _from_row(self, row) -> Result:
        serializer = self.get_serializer(row["dtype"])
        return Result(
            name=row["name"],
            task_name=row["task"],
            data=serializer.loads(row["data"]),
            dtype=row["dtype"],
            last_updated=dt.datetime.strptime(row["last_updated"], _DATETIME_FMT),
            expiry=dt.datetime.strptime(row["expiry"], _DATETIME_FMT) if row["expiry"] else None,
            extra=json.loads(row["extra"]),
        )

    def close(self):
        """Close the database connection."""
        self._conn.close()
        return
