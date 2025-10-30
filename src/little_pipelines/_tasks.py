"""Tasks"""

import datetime as dt
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Literal, Optional, Self, TYPE_CHECKING

from loguru import _Logger

from . import expiry
from ._hashing import hash_file, hash_files, hash_script
from ._logger import make_logger

if TYPE_CHECKING:
    from ._pipeline import Pipeline  # BUG: doesn't work as expected


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

    @property
    def name(self) -> str:
        """Task name"""
        return self._name

    @property
    def is_executed(self) -> bool:
        return self._executed

    @property
    def skipped(self):
        return self._skipped

    @skipped.setter
    def skipped(self, value: bool):
        if value is True:
            self.log(f"Skipping execution of '{self.name}'", "WARNING")
        self._skipped = value

    # @property
    # def disable_logging(self):
    #     return self._disable_logging

    # @disable_logging.setter
    # def disable_logging(self, value: bool):
    #     if value is True:
    #         self.log(f"Logging disabled for '{self.name}'", "WARNING")
    #     self._disable_logging = value

    @property
    def dependencies(self) -> dict[str, Self] | None:
        """Up-stream tasks this task depends on."""
        if self.pipeline is not None:
            return {
                name: self.pipeline.get_task(name) for name in self._dependency_names
            }
        return None

    @property
    def result(self) -> Any:
        if self.pipeline is None:
            return None
        return self.pipeline.cache.get(self.name)

    @property
    def _script_hash(self):
        return hash_script(self)

    @property
    def _inputs_hash(self):
        if self.input_files:
            return hash_files(self)
        return ""

    # ========================================================================
    # Instance methods

    def __init__(
            self: Self,
            name: str,
            dependencies: Optional[list[str]] = None,
            expires: Optional[Callable] = None,
            input_files: Optional[list[Path | str]] = None,
            hash_inputs: bool = True,
            log_path: Optional[Path|Literal["DISABLE"]] = None
        ):
        """
        Initialize a Task.

        Args:
            name: Unique task name
            dependencies: List of task names this task depends on
            execution_type: "AUTO" or "MANUAL" execution
            use_cache: If True, task results will be cached for resume
            input_files: List of input file paths/patterns for hash tracking
            hash_inputs: If False, use empty string hash (for API/DB inputs)
        """
        self._name: str = name
        self._dependency_names: list[str] = dependencies if dependencies else list()
        self.expiry = expires or expiry.session(name)
        self._log_path = (
            log_path
            or Path().home() / ".little_pipelines" / "logs" / f"{self.name}.log"
        )

        self._process_times = []
        self._executed = False
        self._skipped = False

        # Pipeline cache configuration
        self.input_files = input_files  # TODO: ??
        self.hash_inputs = hash_inputs  # TODO: ??

        self.pipeline: Optional["Pipeline"] = None

        # Logging setup
        self._disable_logging = False
        self.logger = None
        if self._log_path != "DISABLE":
            self.logger: _Logger = make_logger(
                self.name,
                filename=self._log_path
            )

    def log(self, msg, level="INFO"):
        if self.logger is None: # or self.disable_logging:
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
            if func_name == "run":
                self._executed = True
            return result

        # Register the custom process with the Task
        setattr(self, func.__name__, _wrapper)
        return

    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self._name}')>"
