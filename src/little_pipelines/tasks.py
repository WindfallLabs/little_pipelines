"""
Tasks
"""

import datetime as dt
from collections.abc import Callable
from functools import cached_property, wraps
from inspect import currentframe
from pathlib import Path
from time import perf_counter_ns
from types import ModuleType
from typing import Any, Optional, Literal, Self, TYPE_CHECKING

from . import _messages as msg
from . import _autodoc, util
#from .cache import Cache, CachedResult, default_cache
from .caching import Cache, CacheResult, default_cache
from ._exceptions import DependencyFailure
from ._hashing import hash_file, hash_files

if TYPE_CHECKING:
    from ._pipeline import Pipeline


def find_tasks(vars: dict[str, Any], nested=True):
    """Finds tasks - useful to `add` all Tasks to a Pipeline."""
    found_instances = set()

    for _, obj in vars.items():
        if isinstance(obj, Task):
            found_instances.add(obj)
        elif isinstance(obj, ModuleType) and nested:
            for attr_name in dir(obj):
                try:
                    attr = getattr(obj, attr_name)
                    if isinstance(attr, Task):
                        found_instances.add(attr)
                except (AttributeError, Exception):
                    continue

    return found_instances


class Task:
    """Parent class for Tasks."""
    def __init__(
            self: Self,
            name: str,
            dependencies: Optional[list[str]] = None,
            if_upstream_errors: Literal["FAIL", "SKIP"] = "FAIL",
            input_files: Optional[list[Path | str]] = None,
            hash_inputs: bool = True,
            cache: Optional[Cache] = None,
            result_expiry: Optional[dt.datetime|dt.date] = None,
            cache_results: bool = False,
            manual_execution_only: bool = False,
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
            cache: Uses Pipeline's cache, default cache, or user-provided cache
            cache_results: Allow the task to save its results to the cache
        """
        self._name: str = name
        self._dependency_names: list[str] = dependencies if dependencies else list()
        self.if_upstream_errors = if_upstream_errors

        # Flags for pipeline
        self.manual_execution_only = manual_execution_only

        self._process_times = []
        self._executed = False
        self._skipped = False

        # Inspection
        # TODO: make method?
        self._g = currentframe().f_back.f_globals
        # Get the docstring for the instance's script
        #self.info = _g.get('__doc__')
        # Get the filepath of the instance's script
        self._script_path = self._g.get('__file__')

        # Hashing
        self.input_files = input_files  # TODO: ??
        self.hash_inputs = hash_inputs  # TODO: ??

        # Pipeline
        self._pipeline: Optional["Pipeline"] = None
        # Initialize a pipeline-independent cache
        self._cache: Cache = default_cache if cache is None else cache
        self._do_cache_results = cache_results
        self.result_expiry = result_expiry  # NOTE: None

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
        try:
            #self.logger.debug(f"Skipped: {value}")
            #self.message.write(self.name, f"Skipped {value}")
            True  # TODO: not sure what callback is useful here
        except AttributeError:
            pass
        self._skipped = value

    @property
    def message(self):
        # Console messaging
        if self.pipeline:
            return self.pipeline.message
        return msg.Message(len(self.name))

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
        # TODO: consider logging
        return

    @property
    def cache(self):
        """Return whatever cache is associated with the task."""
        if self._pipeline:
            return self.pipeline.cache
        return self._cache

    # @property
    # def logger(self):
    #     """Return whatever logger is associated with the task."""
    #     if self._pipeline:
    #         return self.pipeline.logger
    #     return self._logger

    @property
    def result(self) -> Any:
        return self.cache.get(self.name).data

    @property
    def _script_hash(self):
        try:
            return hash_file(self._script_path)
        except:
            return ""

    @property
    def _inputs_hash(self):
        h = ""
        if self.input_files and self.hash_inputs:
            for inp in self.input_files:
                h = hash_files(inp)
        return h

    def get_info(self) -> tuple[str, str]:
        """
        Returns the task definition's (script) docstring and auto-documented function info.
        """
        return (self._g.get("__doc__"), _autodoc(self))

    def get_task_result(self, task_name: str):
        """Allow tasks to access other task results."""
        try:
            return self.cache.get(task_name).data
        except Exception as e:
            pass
        return self.pipeline.get_result(task_name)

    # ========================================================================
    # Decorators

    def process(self, func: Callable) -> None:
        """Wrapper for custom functions."""
        @wraps(func)
        def _process_wrapper(*args, **kwargs) -> None:
            """This code wraps around and executes the wrapped function."""
            
            # ----------------------------------------------------------------
            # Pre-function call
            # ----------------------------------------------------------------
            # Start the process timer
            _start = perf_counter_ns()

            # Get the wrapped function's name
            func_name: str = func.__name__

            # Is associated with a pipeline
            has_pipeline = self.pipeline is not None

            # Set cache-related vars
            cached_result = None
            has_cached_result = False

            if func_name == "run" and has_pipeline:
                # Print task-start message
                self.message.write(self.name, f"Running {self.name}...", **msg.TASK_START)
                # ------------------------------------------------------
                # TODO: should we do this here or at the pipeline-level?
                # Check if cached data
                if self._do_cache_results:
                    with self.message.console.status(f"{self.name}: Checking cache..."):
                        if self.name in self.cache.keys():
                        #try:
                            # Attempt to get previously cached results
                            result_obj: CacheResult = self.cache.get(self.name)
                            # TODO: a cache_load_callback
                            cached_result = result_obj.data
                            has_cached_result = cached_result is not None
                        #except KeyError:
                        #    pass
                    if has_cached_result:
                        self.message.write(self.name, "Loaded cached result", **msg.WARN)
                        self.is_skipped = True
            else:
                # Print process-start message for each non-"run" function
                self.message.write(self.name, f"Running {func_name}...", **msg.PROCESS_START)

            # ----------------------------------------------------------------
            # Wrapped function call
            # ----------------------------------------------------------------
            if func_name == "run":
                if has_cached_result:
                    result = cached_result
                else:
                    result = func(self, *args, **kwargs)
            else:
                with self.message.console.status(f"{self.name}: Running {func_name}..."):
                    result = func(self, *args, **kwargs)

            # ----------------------------------------------------------------
            # Post-function call
            # ----------------------------------------------------------------
            # Cache results, if not already cached
            if func_name == "run" and not has_cached_result:  # Don't re-cache
                try:
                    with self.message.console.status(f"{self.name}: Caching result..."):
                        self.cache.set(self.name, result)  # TODO: expiry=self.result_expiry ??
                except Exception as e:
                    self.message.write(msg=f"Failed to cache data ({type(result).__name__})", **msg.FAIL)
                    self.message.write(msg=e, **msg.FAIL)

            # Sum the process duration
            _time = util.time_diff(_start, perf_counter_ns())
            _time_msg = f"(completed in {_time})"
            self._process_times.append((func_name, _time))

            if func_name == "run":
                self._executed = True
                self.message.write(self.name, _time_msg, **msg.TASK_COMPLETE)
            else:
                self.message.write(self.name, _time_msg, **msg.PROCESS_COMPLETE)
            return result

        # Register the custom process with the Task
        setattr(self, func.__name__, _process_wrapper)
        return

    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self._name}')>"
