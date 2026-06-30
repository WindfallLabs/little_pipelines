"""
Tasks
"""

import datetime as dt
from collections.abc import Callable
from functools import wraps
from inspect import currentframe
from time import perf_counter_ns
from types import ModuleType
from typing import Any, Optional, Literal, Self, TYPE_CHECKING

from . import _messages as msg
from . import _autodoc, util
from .caching import Cache, CacheResult  # TODO: rm default_cache
from .exc import PipelineNotSetError
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
        cache: Optional[Cache] = None,
        result_expiry: Optional[dt.datetime|dt.date] = None,
        use_cached_results: bool = True,
        manual_execution_only: bool = False,
    ):
        """
        Initialize a Task.

        Args:
            name: Unique task name (e.g. MyTask)
            dependencies: List of task names this task depends on
            input_files: List of input file paths/patterns for hash tracking
            hash_inputs: If False, use empty string hash (for API/DB inputs)
            cache: Uses Pipeline's cache, default cache, or user-provided cache
            cache_results: Allow the task to save its results to the cache
        """
        self._name: str = name
        self._dependency_names: set[str] = set(dependencies) if dependencies else set()
        self.if_upstream_errors = if_upstream_errors

        # Flags for pipeline
        self.manual_execution_only = manual_execution_only

        self._process_times = []
        self._executed = False
        self._skipped = False

        # Overridables
        self._cache_read_callback = self._default_cache_read_callback

        # Inspection
        # TODO: make method?
        self._g = currentframe().f_back.f_globals
        # Get the filepath of the instance's script
        self._script_path = self._g.get('__file__')

        # Pipeline
        self._pipeline: Optional["Pipeline"] = None
        # Initialize the cache stuff ....
        self.cache: Cache = cache
        self.use_cached_results = use_cached_results
        self.result_expiry = result_expiry  # NOTE: None

    # ========================================================================
    # Properties

    @property
    def _script_hash(self):
        try:
            return hash_file(self._script_path)
        except:
            return ""

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
            # This essentially gets the longest task name; handles errors
            return self.pipeline.message
        return msg.Message(len(self.name))

    @property
    def dependencies(self) -> dict[str, Self] | None:
        """Up-stream tasks this task depends on."""
        if self.pipeline is not None:
            return {
                name: self.pipeline.get_task(name) for name in self._dependency_names
            }
        raise PipelineNotSetError(f"Dependencies of Task '{self.name}' cannot be determined without a Pipeline")
        #warn(f"Task '{self.name}' is not associated with a Pipeline")
        #return dict()  # TODO: is there a benefit to a warning and returning and empty dict??

    @property
    def pipeline(self):
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: "Pipeline") -> None:
        self._pipeline = pipeline
        # Additional on-add hooks
        # TODO: consider logging
        return

    # @property
    # def logger(self):
    #     """Return whatever logger is associated with the task."""
    #     if self._pipeline:
    #         return self.pipeline.logger
    #     return self._logger

    @property  # TODO: remove??
    def result(self) -> Any:
        """Accessor for the Task's result(s)."""
        return self.cache.get(self.name).data

    def get_result(self, details=False, run_if_not_cached=False, **run_kwargs) -> Any|CacheResult:
        """
        Gets the Task's result(s).

        Args:
            details (bool): Returns the result as a CacheResult
            run_if_not_cached (bool): Runs the task if the results are not already cached and returns the results of that process
        """
        if run_if_not_cached and (not self.cache or self.name not in self.cache.keys()):
            return self.run(**run_kwargs)
        else:
            r: CacheResult = self.cache.get(self.name)
        if details:
            return r
        return r.data

    def _default_cache_read_callback(self, cached_result: CacheResult) -> Any:
        """The default cache-read callback."""
        return cached_result.data

    def on_cache_read(self, func: Callable):
        """Decorator used to override the cache-read callback."""
        from types import MethodType
        self._cache_read_callback = MethodType(func, self)
        return func

    def cache_read_callback(self, cached_result: CacheResult):
        return self._cache_read_callback(cached_result)

    def get_info(self) -> tuple[str, str]:
        """
        Returns the task definition's (script) docstring and auto-documented function info.
        """
        return (self._g.get("__doc__"), _autodoc(self))

    # TODO: deprecate; replace uses with `task.get_dependency("TASK").get_result()`
    def get_dependency_result(self, task_name: str, check_dependency=True):
        """
        Gets another Task's cached result(s).
        
        Args:
            task_name (str): The name of the Task's data to retrieve
            check_dependency (bool): A safety measure to ensure the retrieved data is a dependency
                i.e. The data exists before the calling Task
        """
        # Ensure the accessed task is a dependency
        if task_name in self._dependency_names or check_dependency is False:
            if self.cache is not None:
                try:
                    return self.cache.get(task_name).data
                except KeyError as e:
                    raise e  # TODO: execute dependency?
            else:
                raise AttributeError(f"{self.name} task has no cache set")
        raise KeyError(f"{task_name} is not a dependency of {self.name}")

    def get_dependency(self, task_name: str) -> "Task":
        """
        Gets a dependency (Task object).
        
        Args:
            task_name (str): The name of the Task to retrieve
        """
        if not self.pipeline:
            raise PipelineNotSetError(f"Dependencies of Task '{self.name}' cannot be determined without a Pipeline")
        try:
            return self.dependencies[task_name]
        except KeyError:
            raise KeyError(f"{task_name} is not a dependency of {self.name}")

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

            # Set cache-related vars
            cached_result: Any|None = None

            if func_name == "run":  #  and has_pipeline
                # Print task-start message
                self.message.write(self.name, f"Running {self.name}...", **msg.TASK_START)
                # ------------------------------------------------------
                # Check if cached data
                if self.use_cached_results and self.cache is not None and self.name in self.cache.keys():
                    with self.message.console.status(f"{self.name}: Checking cache..."):
                        # Attempt to get previously cached results
                        result_obj: CacheResult = self.cache.get(self.name)
                    cached_result: Any|None = self.cache_read_callback(result_obj)
                    if cached_result is not None:
                        self.message.write(
                            self.name,
                            f"Loaded cached result ({type(cached_result).__name__})",
                            **msg.WARN
                        )
                        self.is_skipped = True
                elif self.cache is None:
                    self.message.write(self.name, "No Cache set", **msg.WARN)
            else:
                # Print process-start message for each non-"run" function
                self.message.write(self.name, f"Running {func_name}...", **msg.PROCESS_START)

            # ----------------------------------------------------------------
            # Wrapped function call
            # ----------------------------------------------------------------
            if func_name == "run":
                if cached_result is not None:
                    result = cached_result
                else:
                    result = func(self, *args, **kwargs)
            else:
                with self.message.console.status(f"{self.name}: Running {func_name}..."):
                    result = func(self, *args, **kwargs)

            # ----------------------------------------------------------------
            # Post-function call
            # ----------------------------------------------------------------
            # Cache results, if not already cached and if a cache was set
            if func_name == "run" and cached_result is None:  # Don't re-cache
                try:
                    with self.message.console.status(f"{self.name}: Caching result..."):
                        # Process extras
                        extra = None
                        if hasattr(self, "extra"):
                            extra = self.extra
                        self.cache.set(self.name, result, extra)
                except Exception as e:
                    self.message.write(msg=f"Failed to cache data ({type(result).__name__})", **msg.FAIL)
                    self.message.write(msg=e, **msg.FAIL)

            # Sum the process duration
            _time = util.time_diff(_start, perf_counter_ns())
            _time_msg = f"(completed in {_time})"
            self._process_times.append((func_name, _time))

            if func_name == "run":
                if cached_result is not None:
                    # Print "DONE" in grey if cached data was used
                    self.message.write(self.name, _time_msg, **msg.PROCESS_COMPLETE)
                else:
                    # Print "DONE" in green
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
