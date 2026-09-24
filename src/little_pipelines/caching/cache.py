"""
Cache - Result persistence.
"""

import datetime as dt
import json
import sqlite3
from pathlib import Path
from typing import Any, Callable, Literal, Optional

from ..exc import *
from ..pipeline_run import PipelineRun
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

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    tasks_executed INTEGER NOT NULL,
    tasks_skipped INTEGER NOT NULL,
    tasks_failed INTEGER NOT NULL,
    extra TEXT
);
"""


class Cache:
    """
    The Cache object represents an SQLite database where Results and PipelineRuns are stored.
    Its primary function is for sharing access to Results between Tasks.
    """
    def __init__(self, database_path: str|Path = ":memory:"):
        """
        Initialize a Cache.

        Args:
            database_path (str or Path): Path to an SQLite database; default ':memory:'
        """
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

    def _setup_database(self) -> None:
        """
        Ensures that all tables exist and options are set.
        """
        if self._database_path in ("memory", ":memory:"):
            # Creates a shared in-memory database
            self.database_path = "file:cachedb?mode=memory"  # TODO: &cache=shared ?
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
        _ = self._conn.execute("VACUUM;").fetchall()

        return

    # ========================================================================
    # Results

    def get(self, result_name: str, return_raw_rows=False) -> Result | list[dict[str, Any]]:
        """
        Get a Result from the cache.
        """
        # Allow * wildcards ('*' -> '%')
        result_name = result_name.replace("*", "%") if "*" in result_name else result_name

        rows = (
            self._conn.execute(
                "SELECT * FROM cache WHERE name LIKE ?", (result_name,)
            )
            .fetchall()
        )

        if return_raw_rows:
            return [dict(r) for r in rows]

        results: list[Result] = []
        for row in rows:
            results.append(
                self._from_row(row)
            )
        if len(results) == 0:
            if result_name not in self.keys():
                #raise sqlite3.OperationalError(f"No such Result: {result_name}")
                raise ResultNotFoundError(f"Not found: {result_name}")

        return results[0]

    def get_for_task(self, task_name: str) -> list[Result]:
        """
        Return all Results for a given Task.
        """
        # Allow * wildcards ('*' -> '%')
        task_name = task_name.replace("*", "%") if "*" in task_name else task_name

        rows = (
            self._conn.execute(
                "SELECT * FROM cache WHERE task LIKE ?", (task_name,)
            )
            .fetchall()
        )

        results: list[Result] = []
        for row in rows:
            results.append(
                self._from_row(row)
            )

        return results

    def put(self, result: Result, mode: Literal["UPSERT", "IGNORE", "FAIL"] = "UPSERT") -> None:
        """
        Insert a Result into the cache.

        Args:
            result
            mode (
        """
        if type(result) is not Result:
            raise TypeError("This method only accepts Result objects")
        serializer: Serializer = self.get_serializer(result.dtype)
        mode = mode.upper()
        if mode not in {'UPSERT', 'IGNORE', 'FAIL'}:
            raise ValueError("Mode must be one of 'UPSERT', 'IGNORE', or 'FAIL'")
        if mode == "UPSERT":
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache (name, task, dtype, last_updated, expiry, data, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                self._to_row(result)
            )
        elif mode == "FAIL":
            try:
                self._conn.execute(
                    """
                    INSERT INTO cache (name, task, dtype, last_updated, expiry, data, extra)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._to_row(result)
                )
            except sqlite3.IntegrityError:
                #raise sqlite3.IntegrityError(f"{result.name} already in cache")
                raise ResultExistsError(f"Result exists in Cache: {result.name}")
        self._conn.commit()

        return

    def keys(self) -> list[str]:
        """
        Return the names of data within the cache.
        """
        rows = self._conn.execute("SELECT name FROM cache").fetchall()
        return sorted([i[0] for i in rows])

    def clear(self, name: Optional[str] = None) -> bool:
        """
        Clear a record from the cache, or rebuilds the cache table.
        """
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
            return True

    def serializer(self, type_arg: type) -> Callable:
        """
        Decorator to register a CacheSerializer subclass.

        Args:
            type_arg: Type to associate with the serializer.

        Returns:
            A decorator function that registers the serializer class.

        Example:
            ```
            import sys

            cache = Cache("cache.sqlite")

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
        """
        Return a data serializer.
        """
        return self._serializers.get(dtype, self._default_serializer)


    def _to_row(self, result: Result) -> tuple:
        """
        Prepares a Result for insert by serializing data and converting data to a tuple.
        """
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
        """
        Convert
        """
        serializer = self.get_serializer(row["dtype"])
        r = Result(
            name=row["name"],
            task_name=row["task"],
            data=serializer.loads(row["data"]),
            dtype=row["dtype"],
            last_updated=dt.datetime.strptime(row["last_updated"], _DATETIME_FMT),
            expiry=dt.datetime.strptime(row["expiry"], _DATETIME_FMT) if row["expiry"] else None,
            extra=json.loads(row["extra"]),
        )

        return r

    def close(self) -> None:
        """
        Close the database connection.
        """
        self._conn.close()
        return

    # ========================================================================
    # Pipeline Runs
    def put_run(self, run: PipelineRun) -> None:
        """
        Store a PipelineRun.
        """
        record = run.to_record()

        self._conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs (
                run_id,
                pipeline_name,
                start_time,
                end_time,
                tasks_executed,
                tasks_skipped,
                tasks_failed,
                extra
            )
            VALUES (
                :run_id,
                :pipeline_name,
                :start_time,
                :end_time,
                :tasks_executed,
                :tasks_skipped,
                :tasks_failed,
                :extra
            )
            """,
            record,
        )

        self._conn.commit()
        return

    def get_runs(self, pipeline_name: str | None = None) -> list:
        """
        Return PipelineRuns ordered newest-first.
        """
        cursor = self._conn.cursor()

        if pipeline_name:
            cursor.execute(
                """
                SELECT *
                FROM pipeline_runs
                WHERE pipeline_name = ?
                ORDER BY start_time DESC
                """,
                (pipeline_name,),
            )
        else:
            cursor.execute(
                """
                SELECT *
                FROM pipeline_runs
                ORDER BY start_time DESC
                """
            )

        rows = cursor.fetchall()
        columns = [
            desc[0]
            for desc in cursor.description
        ]
        runs = []

        for row in rows:
            record = dict(
                zip(columns, row)
            )
            runs.append(
                PipelineRun.from_record(record)
            )

        return runs

    def get_last_run(self, pipeline_name: str | None = None) -> PipelineRun | None:
        """
        Return the most recent PipelineRun.
        """

        runs = self.get_runs(
            pipeline_name=pipeline_name
        )

        if not runs:
            return None

        return runs[0]

    def clear_runs(self, pipeline_name: str | None = None) -> None:
        """
        Delete stored PipelineRun records.
        """
        if pipeline_name:
            self._conn.execute(
                """
                DELETE FROM pipeline_runs
                WHERE pipeline_name = ?
                """,
                (pipeline_name,),
            )
        else:
            self._conn.execute(
                """
                DELETE FROM pipeline_runs
                """
            )
        self._conn.commit()

        return


__all__ = ["Cache"]

