"""
Tasks
"""

import datetime as dt
import inspect
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from functools import wraps
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


class DependencyDict(dict):
    def __init__(self, d: dict = None):
        if d is None:
            d = {}
        super().__init__(d)
    
    # def __get__(self, obj, objtype=None):
    #     if obj is None:
    #         return self
    #     return self
    
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError as e:
            raise KeyError(f"Dependency '{key}' not in Task.dependencies list") from e


class Task:
    """Parent class for Tasks."""
    def __init__(
        self: Self,
        name: str,
        cache: Optional[Cache] = None,
        dependencies: Optional[list[str]] = None,

        # TODO: WIP parameters
        outputs: Optional[dict[str, type]] = None,
        if_upstream_errors: Literal["FAIL", "SKIP"] = "FAIL",
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

        if outputs is not None and not all(isinstance(k, str) for k in outputs.keys()):
            raise AttributeError
        self.outputs = {self._name: Any}
        if outputs:
            self.outputs = outputs

        self.if_upstream_errors = if_upstream_errors

        # Flags for pipeline
        self.manual_execution_only = manual_execution_only

        self._process_times = []
        self._executed = False
        self._skipped = False
        self._has_errors = False

        # Overridables
        self._cache_read_callback = self._default_cache_read_callback

        # Inspection
        # The script the Task is initialized in
        module = inspect.currentframe().f_back
        self._g = module.f_globals
        # Get the filepath of the instance's script
        self._script = inspect.getmodule(module)
        self._script_path = self._g.get('__file__')

        # Pipeline
        self._pipeline: Optional["Pipeline"] = None
        self._quiet = True  # Task-specific
        self._raise_errors = True
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
        return self._executed and not self._has_errors

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
        return msg.Message(None, len(self.name), self._quiet)

    @property
    def dependencies(self) -> dict[str, Result] | None:
        """Results of upstream Tasks that this Task depends on."""
        # Create _dependencies if it is None
        if not self._dependencies:
            deps: dict[str, Any] = {}
            for d in self._dependency_names:
                try:
                    dep: Result = self.cache.get(result_name=d)[0]
                    deps[d] = dep
                except IndexError:
                    raise DependencyNotFoundError(f"'{d}' not in cache")

            self._dependencies = DependencyDict(deps)

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

    def result(self, data: Any, name: Optional[str] = None) -> Result:  # TODO: consider fulfilling a Data object instead
        """
        Creates a Result object.
        """
        # If none, the result name is set to the task name
        if not name:
            name = self.name

        r = Result(
            name=name,
            data=data,
            task_name=self.name,
            # TODO: expiry and extra
        )
        return r

    def get_result(self, details=False, run_if_not_cached=False, **run_kwargs) -> Any|Result:  # TODO: deprecate?
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

    # TODO: rename to _get_task and allow it to get tasks outside of dependencies
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

    @contextmanager  # TODO: replace with util.Timer or better, util.process_timer
    def _timed(self, func_name: str, complete_kind: dict):
        """Times a block, records it, and prints the completion message."""
        _start = perf_counter_ns()
        yield
        _time = util.time_diff(_start, perf_counter_ns())
        self._process_times.append((func_name, _time))
        if not self._has_errors:
            self.message.write(self.name, f"(completed in {_time})", **complete_kind)
        return

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

    def _load_cached_results(self) -> Any | tuple[Any]:  # TODO: or do we WANT the Result objects?
        """
        Returns all cached data for this task.
        """
        if self.cache is None:
            self.message.write(self.name, "No Cache set", **msg.WARN)
            raise AttributeError("No cache set.")

        results: tuple[Any] = tuple([r.data for r in self.cache.get(task_name=self.name)])
        if len(results) == 1:
            results: Any = results[0]
        return results

    def _resultify(self, return_values: Any) -> tuple[Result]:
        """
        Forces the values returned by 'main' into a tuple of results.
        """
        # Handle single Result
        if isinstance(return_values, Result):
            if not return_values.task_name:
                return_values.task_name = self.name
            return (return_values,)
        
        # Handle Sequence (but not string)
        if isinstance(return_values, Sequence) and not isinstance(return_values, str):
            if not all(isinstance(item, Result) for item in return_values):
                raise TypeError("Sequence must contain only Result objects")
            return tuple(return_values)
        
        # Handle any other single value, and inherit task.name
        return (Result(data=return_values, name=self.name, task_name=self.name),)

    def _cache_and_return_result_data(self, results: tuple[Result]) -> tuple[Any]:
        """
        Handles return values as Results and puts them in the Cache.
        """
        # Check result names for uniqueness
        result_names: list[str] = [r.name for r in results]
        if len(set(result_names)) != len(result_names):
            raise ValueError("Multiple Results have the same name")

        unpacked_data: list[Any] = []
        return_data: Any | tuple[Any]
        for result in results:
            try:
                self.cache.put(result)
            except Exception as e:
                self._has_errors = True
                self.message.write(self.name, msg=f"Failed to cache data ({type(result).__name__})", **msg.FAIL)
                self.message.write(self.name, msg=f"{e.__class__.__name__}: {e}", **msg.FAIL)
            unpacked_data.append(result.data)

        # Return the contents of the tuple if there's only one  # TODO: good idea?
        if len(unpacked_data) == 1:
            return_data = unpacked_data[0]
        else:
            return_data = tuple(unpacked_data)

        return return_data

    def main(self, func: Callable) -> None:
        """
        Wraps the task's user-defined 'main' function.

        kwargs that can be passed to the user-function:
            force (bool): Force the execution of the task (default True)
            quiet (bool): 
            raise_errors (bool): 
        """
        @wraps(func)
        def _main_wrapper(*args, **kwargs) -> Any | tuple[Any]:
            """Little Pipelines' secret sauce."""
            kwargs_allowed = [
                "force",
                "quiet",
                "raise_errors",
            ]
            self.message.write(self.name, f"Running {self.name}...", **msg.TASK_START)

            # Process / handle kwargs
            # NOTE: these options are mostly for use within a shell
            # Force execution of a task
            force: bool = kwargs.get("force", True)
            if force not in (True, False):
                raise AttributeError("'force' kwarg must be bool")
            # Optionally quiet a task
            quiet: bool = kwargs.get("quiet", True)
            if quiet not in (True, False):
                raise AttributeError("'quiet' kwarg must be bool")
            if quiet != self._quiet:
                self._quiet = quiet
            # Ignoring errors allows the pipeline to continue running if some tasks fail
            raise_errors: bool = kwargs.get("raise_errors", True)
            if raise_errors not in (True, False):
                raise AttributeError("'raise_errors' kwarg must be bool")
            if raise_errors != self._raise_errors:
                self._raise_errors = raise_errors

            # Clean kwargs
            kwargs: dict = {k: v for k, v in kwargs.items() if k not in kwargs_allowed}

            with self._timed(func.__name__, msg.TASK_COMPLETE):
                # Attempt to get cached data
                #if self.use_cached_results:
                if force is False:
                    r: tuple[Result] = self._load_cached_results()
                    if r is not None:
                        #self._skipped = True  # TODO: is this skipping?
                        return r

                # Run the main function
                try:
                    return_values: Any | tuple[Result] = func(self, *args, **kwargs)
                except Exception as e:
                    self._has_errors = True
                    self.message.write(self.name, msg=f"Failed to run function 'main/{func.__name__}'", **msg.FAIL)
                    self.message.write(self.name, msg=f"{e.__class__.__name__}: {e}", **msg.FAIL)
                    # TODO: print some sort of traceback
                    if raise_errors is True:
                        raise e
                    return

                # Cache the results
                results: tuple[Result] = self._resultify(return_values)

                # Process the returned data as result objects
                unpacked_data: Any | tuple[Any] = self._cache_and_return_result_data(results)
                self._executed = True

                return unpacked_data

        setattr(self, "main", _main_wrapper)
        return


    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self._name}')>"
