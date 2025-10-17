"""Tasks"""

import datetime as dt
from collections.abc import Callable
from functools import wraps
from time import perf_counter_ns
from types import MappingProxyType
from typing import Any, Literal, Optional, Self

from loguru import _Logger

from ._config import config
from ._exceptions import TaskNotFoundError
from ._logger import make_logger


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg


class Task:
    """Parent class for Tasks."""
    registry: list[Self] = []
    show_warnings = True

    # ========================================================================
    # Class methods & dunders

    @classmethod
    def get_task(cls, name: str):
        try:
            ds = [i for i in cls.registry if i.name == name][0]
            return ds
        except IndexError:
            raise TaskNotFoundError(f"Task '{name}' not found in registry")

    def __repr__(self):
        return f"<{self.__class__.__name__}(name='{self._name}')>"

    # ========================================================================
    # Properties

    @property
    def name(self) -> str:
        """Task name"""
        return self._name

    @property
    def dependencies(self) -> list[Self]:
        """Up-stream tasks this task depends on."""
        return [
            self.get_task(n) for n in self._dependency_names
        ]

    @property
    def dependents(self) -> list[Self]:
        """Down-stream tasks that depend on this task."""
        dependents = [
            i[0] for i in [
                (t, t._dependency_names) for t in Task.registry
            ] if self.name in i[1]]
        return dependents

    @property
    def datastore(self) -> dict[str, Any]:
        return MappingProxyType(self._datastore)
    
    @property
    def has_data(self) -> bool:
        return len(self.datastore.keys()) > 0

    # ========================================================================
    # Instance methods

    def __init__(
            self: Self,
            name: str,
            dependencies: Optional[list[str]] = None,
            execution_type: Literal["AUTO", "MANUAL"] = "AUTO"
        ):
        self._name: str = name
        self._dependency_names: list[str] = dependencies if dependencies else list()
        if execution_type.upper() not in ("AUTO", "MANUAL"):
            raise AttributeError("'execution_type' must either be 'AUTO' or 'MANUAL'")
        self._execution_type = execution_type.upper()
        self._process_times = []
        # A place to store data for the lifecycle of the object
        self._datastore: dict[str, Any] = {}
        self._executed = False

        # Logging setup
        self.logger: _Logger = make_logger(
            self.name,
            filename=config.log_dir
        )

        # Add the instance to the registry
        self.registry.append(self)

    def store(self, key: str, value: Any) -> None:
        """Add a key-value pair to the data store."""
        self._datastore.update({key: value})
        return
    
    def flush(self):
        """Flush the data store."""
        self._datastore.clear()
        return

    def log(self, msg, level="INFO"):
        if not self.logger:
            return
        self.logger.opt(colors=True).log(level, msg)
        return

    # ========================================================================
    # Decorators

    def process(self, func: Callable) -> None:
        """Wrapper for custom functions."""
        @wraps(func)
        def _wrapper(*args, **kwargs) -> None:
            func_name: str = func.__name__
            # Start the process timer
            _start = perf_counter_ns()
            self.log(
                f"<light-black>{self.name}:{func_name}...</>",
                "DEBUG"
            )
            # Run the process
            result = func(self, *args, **kwargs)
            # Sum the process duration
            _time = time_diff(_start, perf_counter_ns())
            self._process_times.append((func_name, _time))
            self.log(
                f"{self.name}:{func_name} (in {_time})",
                "PERF"
            )
            return result

        # Register the custom process with the Task
        setattr(self, func.__name__, _wrapper)
        return
