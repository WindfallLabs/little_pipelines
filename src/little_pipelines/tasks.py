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

from . import _autodoc, util
from .caching import Cache, Result
from .data import Data
from .exc import PipelineNotSetError, DependencyNotFoundError
from ._hashing import hash_file, hash_files
from .messaging import get_logger

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
        dependencies: Optional[list[str | Data]] = None,

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
            dependencies: Data required for this Task to perform work.
            input_files: List of input file paths/patterns for hash tracking
            hash_inputs: If False, use empty string hash (for API/DB inputs)
            cache: Uses Pipeline's cache, default cache, or user-provided cache
            cache_results: Allow the task to save its results to the cache
            outputs: A validation contract that describes what the Task promises to return.
        """
        self._name: str = name

        # Dependencies
        dep_names = set()
        if dependencies:
            for dep in dependencies:
                if isinstance(dep, Data):
                    dep_names.add(dep.dependency_name())
                else:
                    dep_names.add(str(dep))
        self._dependency_names = frozenset(dep_names)
        #self._dependency_names: frozenset[str] = frozenset(dependencies) if dependencies else set()
        self._dependencies: Optional[dict[str, Any]] = None

        # if outputs is not None and not all(isinstance(k, str) for k in outputs.keys()):
        #     raise AttributeError
        # self.outputs = {self._name: Any}
        # if outputs:
        #     self.outputs = outputs
        self._output_specs = self._normalize_outputs(outputs)

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
        self.script_path = self._g.get('__file__')

        # Pipeline
        self._pipeline: Optional["Pipeline"] = None
        self._quiet = True  # Task-specific
        self._raise_errors = True
        # Initialize the cache stuff ....
        self.cache: Cache = cache
        self.use_cached_results = use_cached_results
        self.result_expiry = result_expiry  # NOTE: None
        self._result_names = set()

        # Logging
        self.logger = get_logger()

    # ========================================================================
    # Properties

    @property
    def outputs(self) -> dict[str, type]:
        """
        Public view of Task outputs.

        Returns the legacy mapping:
            output_name -> dtype
        """

        return {
            name: spec["dtype"]
            for name, spec in self._output_specs.items()
        }

    @property
    def _script_hash(self):
        try:
            return hash_file(self.script_path)
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
            #self.logger.warn(self.name, f"Skipped {value}")
            True  # TODO: not sure what callback is useful here
        except AttributeError:
            pass
        self._skipped = value

    @property
    def dependency_names(self):
        return self._dependency_names

    @property
    def dependencies(self) -> dict[str, Result] | None:
        """Results of upstream Tasks that this Task depends on."""
        # Create _dependencies if it is None
        if not self._dependencies:
            deps: dict[str, Any] = {}
            for d in self._dependency_names:
                try:
                    dep: Result = self.cache.get(d)
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
        return

    def _normalize_outputs(
        self,
        outputs: dict[str, type] | list[Data] | None,
    ) -> dict[str, dict[str, Any]]:
        """
        Normalize Task output declarations into a common structure.

        Returns
        -------
        dict

        Example:

            {
                "Parcels": {
                    "dtype": GeoDataFrame,
                    "validator": parcels.validate,
                    "data": parcels,
                }
            }

        Notes
        -----
        The returned structure is private framework metadata and
        should not be exposed directly to users.
        """

        # Default Task output behavior
        if outputs is None:
            return {
                self.name: {
                    "dtype": Any,
                    "validator": None,
                    "data": None,
                }
            }

        # ---------------------------------------------------------
        # Data objects
        # ---------------------------------------------------------

        if isinstance(outputs, list):
            specs: dict[str, dict[str, Any]] = {}
            for data in outputs:
                if not isinstance(data, Data):
                    raise TypeError(
                        "Task outputs lists must contain only "
                        "Data objects."
                    )

                specs[data.name] = {
                    "dtype": data.dtype,
                    "validator": (
                        data.validate
                        if getattr(data, "_validator", None)
                        else None
                    ),
                    "data": data,
                }

            return specs

        # ---------------------------------------------------------
        # Legacy dict[str, type]
        # ---------------------------------------------------------

        if isinstance(outputs, dict):
            specs: dict[str, dict[str, Any]] = {}
            for name, dtype in outputs.items():
                if not isinstance(name, str):
                    raise TypeError("Output names must be strings.")

                if not isinstance(dtype, type):
                    raise TypeError(f"Output '{name}' must map to a type.")

                specs[name] = {
                    "dtype": dtype,
                    "validator": None,
                    "data": None,
                }

            return specs

        raise TypeError(
            "outputs must be one of:\n"
            "    None\n"
            "    dict[str, type]\n"
            "    list[Data]"
        )

    def _validate_outputs(self, results: tuple[Result]) -> None:
        """
        Validate Task outputs against the declared output contract.

        Checks:

            1. Missing outputs
            2. Unexpected outputs
            3. Output types
            4. Data validators

        Raises
        ------
        ExceptionGroup
            One or more output validation failures.
        """
        errors: list[Exception] = []
        expected = self._output_specs
        actual = {result.name: result for result in results}
        expected_names = set(expected.keys())
        actual_names = set(actual.keys())

        # ============================================================
        # Missing outputs
        # ============================================================

        for missing_name in sorted(expected_names - actual_names):
            errors.append(
                MissingOutputError(f"Missing output: '{missing_name}'")
            )

        # ============================================================
        # Unexpected outputs

        for unexpected_name in sorted(actual_names - expected_names):
            errors.append(
                UnexpectedOutputError(f"Unexpected output: '{unexpected_name}'")
            )

        # ============================================================
        # Type validation + Data validation

        for output_name in (expected_names & actual_names):
            result = actual[output_name]
            spec = expected[output_name]
            dtype = spec["dtype"]
            data_obj = spec["data"]

            # --------------------------------------------------------
            # Type validation

            if (
                dtype is not Any
                and dtype is not None
                and not isinstance(
                    result.data,
                    dtype,
                )
            ):
                dtype_name = getattr(
                    dtype,
                    "__name__",
                    str(dtype),
                )
                errors.append(
                    OutputTypeError(
                        f"Output '{output_name}' "
                        f"returned "
                        f"{type(result.data).__name__}; "
                        f"expected "
                        f"{dtype_name}"
                    )
                )
                # Skip deeper validation when the type is wrong.
                continue

            # --------------------------------------------------------
            # Data validation

            if data_obj is not None:
                try:
                    # Uses Data.validate(...)
                    data_obj.validate(result.data)
                except Exception as e:
                    errors.append(
                        OutputTypeError(
                            f"Validation failed for '{output_name}': {e}"
                        )
                    )

        # ============================================================
        # Final

        if errors:
            raise ExceptionGroup(
                f"Output validation failed for Task '{self.name}'",
                errors,
            )

        return

    def result(self, data: Any, name: Optional[str] = None) -> Result:  # TODO: remove and replace instances with Data.fulfill(value)
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

    def get_results(self, named=False) -> list[Result] | dict[str, Result]:  # TODO: add run_if_not_cached=False, **run_kwargs
        """
        Gets the Task's result(s).

        Args:
            named (bool): Returns a dict[str, Result] if true, list[Results] if False
            details (bool): Returns the result as a Result
            run_if_not_cached (bool): Runs the task if the results are not already cached and returns the results of that process
        """
        # TODO: run on demand?
        #if run_if_not_cached and (not self.cache or self.name not in self.cache.keys()):
        #    return self.run(**run_kwargs)
        #else:
        #    r: Result = self.cache.get(self.name)
        
        results: list[Result] | dict[str, Result]
        results = self.cache.get_for_task(self.name)
        if named:
            results = {r.name: r for r in results}
        return results

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

    def process(self, func: Callable) -> None:
        """Wrapper for method-like custom functions."""
        @wraps(func)
        def _process_wrapper(*args, **kwargs) -> Any:
            self.logger.process_start(self.name, func.__name__)
            with util.process_timer() as _t:
                with self.logger.spinner(f"{self.name}: Running {func.__name__}..."):
                    result = func(self, *args, **kwargs)  # TODO: indent
            return result

        setattr(self, func.__name__, _process_wrapper)
        return

    def _load_cached_results(self) -> Any | tuple[Any]:  # TODO: or do we WANT the Result objects?
        """
        Returns all cached data for this task.
        """
        if self.cache is None:
            self.logger.warn(task=self.name, msg="No Cache set.")
            raise AttributeError("No cache set.")

        results: tuple[Any] = tuple([r.data for r in self.cache.get(task_name=self.name)])
        #if len(results) == 1:
        #    results: Any = results[0]
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
                self.logger.error(task=self.name, msg=f"Failed to cache data ({type(result).__name__})")
                self.logger.error(task=self.name, msg=f"{e.__class__.__name__}: {e}")
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
            self.logger.task_start(self.name)

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

            with util.process_timer() as _t:
                # Attempt to get cached data
                #if self.use_cached_results:
                if force is False:
                    r: list[Result] = self.cache.get_for_task(self.name)
                    if r is not None:
                        #self._skipped = True  # TODO: is this skipping?
                        return r

                # Run the main function
                try:
                    return_values: Any | tuple[Result] = func(self, *args, **kwargs)
                except Exception as e:
                    self._has_errors = True
                    self.logger.error(task=self.name, msg=f"Failed to run function 'main/{func.__name__}'")
                    self.logger.error(task=self.name, msg=f"{e.__class__.__name__}: {e}")
                    # TODO: print some sort of traceback
                    if raise_errors is True:
                        raise e
                    return

                # Get the results
                results: tuple[Result] = self._resultify(return_values)

                # Validate outputs
                self._validate_outputs(
                    results
                )

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
