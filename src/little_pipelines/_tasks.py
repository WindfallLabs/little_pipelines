"""Tasks"""

from collections.abc import Callable
from functools import wraps
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Optional, Self, TYPE_CHECKING

from loguru import _Logger

from . import expire
from . import util
from ._hashing import hash_file, hash_files, hash_script
from ._logger import make_logger

if TYPE_CHECKING:
    from ._pipeline import Pipeline  # BUG: doesn't work as expected


class Task:
    """Parent class for Tasks."""
    def __init__(
            self: Self,
            name: str,
            dependencies: Optional[list[str]] = None,
            expire_results: Optional[Callable] = None,
            input_files: Optional[list[Path | str]] = None,
            hash_inputs: bool = True,
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
        self.expire_results = expire_results or expire.after_session(name)

        self._process_times = []
        self._executed = False
        self._skipped = False

        # Pipeline cache configuration
        self.input_files = input_files  # TODO: ??
        self.hash_inputs = hash_inputs  # TODO: ??

        self._pipeline: Optional["Pipeline"] = None

        # Logging setup - None at init, built at pipeline.add()
        self.logger: Optional[_Logger] = None
        self._enable_logging = True

    def _build_logger(self) -> None:
        if self.logger is None:
            self.logger: _Logger = make_logger(
                self.name,
                filename=self.log_dir if self._enable_logging else None,
            )
        return

    # ========================================================================
    # Properties

    @property
    def name(self) -> str:
        """Task name"""
        return self._name

    @property
    def is_executed(self) -> bool:
        return self._executed

    @property
    def is_skipped(self):
        return self._skipped

    @is_skipped.setter
    def is_skipped(self, value: bool):
        #if value is True:
        #    self.log(f"Skipping execution of '{self.name}'", "WARNING")
        self._skipped = value

    @property
    def dependencies(self) -> dict[str, Self] | None:
        """Up-stream tasks this task depends on."""
        if self.pipeline is not None:
            return {
                name: self.pipeline.get_task(name) for name in self._dependency_names
            }
        return None

    @property
    def pipeline(self):
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: "Pipeline") -> None:
        self._pipeline = pipeline
        # Additional on-add hooks
        self._build_logger()
        self.logger.debug(f"Added to pipeline: '{pipeline.name}'")
        self.logger.debug("Logger built")
        return

    @property
    def log_dir(self) -> Path|None:
        if not self._enable_logging:
            return
        if self.pipeline:
            pipe_name = self.pipeline.name
            return (
                Path().home() / ".little_pipelines"
                / pipe_name / "logs" / f"{self.name}.log"
            )
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
    # Decorators

    def process(self, func: Callable) -> None:
        """Wrapper for custom functions."""
        @wraps(func)
        def _process_wrapper(*args, **kwargs) -> None:
            func_name: str = func.__name__

            # Pre-`run` stuff
            if func_name == "run":
                self.logger.info(f"Running {self.name}...")
                self.logger.debug(f"Expiry: {self.expire_results.__name__}")
            else:
                self.logger.debug(f"<light-black>Running {self.name}:{func_name}</>")

            # Start the process timer
            _start = perf_counter_ns()  # TODO: this should be a context `with PerfCounter:`
            # Run the process
            result = func(self, *args, **kwargs)
            # Sum the process duration
            _time = util.time_diff(_start, perf_counter_ns())
            self._process_times.append((func_name, _time))

            # Post-`run` stuff
            if func_name == "run":
                self._executed = True
                self.logger.success(f"  DONE <light-black>(in {_time})</>")
            else:
                self.logger.success(
                    f"  <light-black>DONE {func_name} (in {_time})</>"
                )
            return result

        # Register the custom process with the Task
        setattr(self, func.__name__, _process_wrapper)
        return

    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self._name}')>"
