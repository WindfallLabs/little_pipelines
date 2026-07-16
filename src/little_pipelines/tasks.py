"""
Tasks
"""

import datetime as dt
from collections.abc import Callable
from contextlib import contextmanager
from functools import wraps
from inspect import currentframe
from time import perf_counter_ns
from types import ModuleType
from typing import Any, Optional, Literal, Self, TYPE_CHECKING

from . import _messages as msg
from . import _autodoc, util
from .caching import Cache, Result
from .exc import PipelineNotSetError, DependencyNotFoundError
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
        self._dependency_names: frozenset[str] = frozenset(dependencies) if dependencies else set()
        self._dependencies: Optional[dict[str, Any]] = None
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
        self._result_names = set()

    # ========================================================================
    # Properties

    @property
    def results(self) -> dict[str|int, Any]:
        """An accessor for the task's cached results."""
        #return self.cache.get(task_name=self.name)
        r: dict[str|int, Any] = {
            r.name: r.data
            for r in self.cache.get(task_name=self.name)
        }
        # Create a list-like 0-index shorthand accessor
        if len(r.keys()) == 1:
            r[0] = r[self.name]
        
        return r

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
    def dependencies(self) -> dict[str, Result] | None:
        """Results of upstream Tasks that this Task depends on."""
        # Create _dependencies if it is None
        if not self._dependencies:
            deps: dict[str, Any] = {}
            for d in self._dependency_names:
                try:
                    dep: Result = self.cache.get(d)[0]
                    deps[d] = dep
                except IndexError:
                    raise DependencyNotFoundError(f"'{d}' not in cache")

            self._dependencies = deps

        return self._dependencies

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

    # @property  # TODO: remove??
    # def result(self) -> Any:
    #     """Accessor for the Task's result(s)."""
    #     return self.cache.get(self.name).data

    def result(self, data: Any, name: Optional[str] = None) -> Result:
        """Creates a Result object."""
        # The default result name is the same as the task
        if not name:
            name = self.name
        if name in self._result_names:
            raise ValueError(f"This task already returns a result with name '{name}'")
        r = Result(
            name=name,
            task=self.name,
            data=data,
            # TODO: expiry and extra
        )
        self._result_names.add(name)
        return r

    def get_result(self, details=False, run_if_not_cached=False, **run_kwargs) -> Any|Result:
        """
        Gets the Task's result(s).

        Args:
            details (bool): Returns the result as a Result
            run_if_not_cached (bool): Runs the task if the results are not already cached and returns the results of that process
        """
        if run_if_not_cached and (not self.cache or self.name not in self.cache.keys()):
            return self.run(**run_kwargs)
        else:
            r: Result = self.cache.get(self.name)
        if details:
            return r
        return r.data

    def _default_cache_read_callback(self, cached_result: Result) -> Any:
        """The default cache-read callback."""
        return cached_result.data

    def on_cache_read(self, func: Callable):
        """Decorator used to override the cache-read callback."""
        from types import MethodType
        self._cache_read_callback = MethodType(func, self)
        return func

    def cache_read_callback(self, cached_result: Result):
        return self._cache_read_callback(cached_result)

    def get_info(self) -> tuple[str, str]:
        """
        Returns the task definition's (script) docstring and auto-documented function info.
        """
        return (self._g.get("__doc__"), _autodoc(self))

    # TODO: deprecate; replace uses with `task.get_dependency("TASK").get_result()`
    def __get_dependency_result(self, task_name: str, check_dependency=True):
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
                    return self.cache.get(task_name)[0].data
                except KeyError as e:
                    raise e  # TODO: execute dependency?
            else:
                raise AttributeError(f"{self.name} task has no cache set")
        raise KeyError(f"{task_name} is not a dependency of {self.name}")

    # TODO: rename to _get_task and allow it to get tasks outside of dependencies
    # There's no reason a user should be manipulating tasks from another task
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

    # TODO: Legacy
    def __process_old(self, func: Callable) -> None:
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
                        result_obj: Result = self.cache.get(self.name)
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
                    self.message.write(self.name, msg=f"Failed to cache data ({type(result).__name__})", **msg.FAIL)
                    self.message.write(self.name, msg=f"{e.__class__.__name__}: {e}", **msg.FAIL)

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

    # ========================
    # TODO: new version (WIP)
    @contextmanager
    def _timed(self, func_name: str, complete_kind: dict):
        """Times a block, records it, and prints the completion message."""
        _start = perf_counter_ns()
        yield
        _time = util.time_diff(_start, perf_counter_ns())
        self._process_times.append((func_name, _time))
        if self._executed:
            self.message.write(self.name, f"(completed in {_time})", **complete_kind)

    def process(self, func: Callable) -> None:
        """Wrapper for method-like custom functions."""
        @wraps(func)
        def _process_wrapper(*args, **kwargs) -> Any:
            self.message.write(self.name, f"Running {func.__name__}...", **msg.PROCESS_START)
            with self._timed(func.__name__, msg.PROCESS_COMPLETE):
                with self.message.console.status(f"{self.name}: Running {func.__name__}..."):
                    result = func(self, *args, **kwargs)
            return result

        setattr(self, func.__name__, _process_wrapper)
        return

    def _load_cached_result(self) -> Result|None:
        """Checks for cached data"""
        if self.use_cached_results and self.cache is not None and self.name in self.cache.keys():
            with self.message.console.status(f"{self.name}: Checking cache..."):
                # Attempt to get previously cached results
                result_obj: Result = self.cache.get(self.name)
            try:
                cached_result: Any|None = self.cache_read_callback(result_obj)
                if cached_result is not None:
                    self.message.write(
                        self.name,
                        f"Loaded cached result ({type(cached_result).__name__})",
                        **msg.WARN
                    )
                    self.is_skipped = True
                    # TODO: we need to return a tuple[Result] if there are multiple results related to TaskName
                    return cached_result
            except Exception as e:  # TODO:
                print("Couldn't load cached data")
                print(e)

        elif self.cache is None:
            self.message.write(self.name, "No Cache set", **msg.WARN)
        return None

    def _process_results(self, results) -> list[Any]:
        """Handles return values as Results and puts them in the Cache."""
        #if not self.cache: ...  # TODO:
        return_data: list[Any] = []
        for r in results:
            if isinstance(r, Result):
                result: Result = r
            # Assume single non-Result value
            else:
                result = Result(
                    name=self.name,
                    task=self.name,
                    data=r,
                )
            try:
                self.cache.put(result)
                self._executed = True
            except Exception as e:
                self.message.write(self.name, msg=f"Failed to cache data ({type(result).__name__})", **msg.FAIL)
                self.message.write(self.name, msg=f"{e.__class__.__name__}: {e}", **msg.FAIL)
            return_data.append(result.data)
        return return_data


    def main(self, func: Callable) -> None:
        """Wraps the task's main function."""
        @wraps(func)
        def _main_wrapper(*args, **kwargs) -> Any:
            self.message.write(self.name, f"Running {self.name}...", **msg.TASK_START)

            with self._timed(func.__name__, msg.TASK_COMPLETE):
                # Run the main function
                try:
                    raw_results: Any|tuple[Any]|tuple[Result] = func(self, *args, **kwargs)
                except Exception as e:
                    self.message.write(self.name, msg=f"Failed to run function 'main'", **msg.FAIL)
                    self.message.write(self.name, msg=f"{e.__class__.__name__}: {e}", **msg.FAIL)
                    return

                # Cache the results
                # TODO: if not use_cache...
                results: tuple[Any]|tuple[Result]
                if type(raw_results) is not tuple:
                    results = (raw_results,)
                else:
                    results = raw_results

                return_data = self._process_results(results)

            return (
                # Return the single instance of original data
                return_data[0] if len(return_data) == 1
                # Or a tuple if many
                else tuple(return_data)
            )

        setattr(self, "main", _main_wrapper)
        return


    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self._name}')>"
