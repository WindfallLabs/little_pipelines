"""
Tasks - The Workers.
"""

import datetime as dt
import inspect
from collections.abc import Callable
from functools import wraps
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, Self

from . import exc, util
from .caching import Cache, Result
from .dataspec import DataSpec
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

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError as e:
            raise exc.DependencyNotFoundError(f"Dependency '{key}' not in Task.dependencies list") from e


class Task:
    """Parent class for Tasks."""
    def __init__(
        self: Self,
        name: str,
        cache: Cache | None = None,
        dependencies: list[str | DataSpec] | None = None,
        outputs: list[DataSpec] | None = None,
        manual_execution_only: bool = False,
        # WIP arguments
        if_upstream_errors: Literal["FAIL", "SKIP"] = "FAIL",
        result_expiry: dt.datetime | dt.date | None = None,
        use_cached_results: bool = True,
    ):
        """
        Initialize a Task.

        Args:
            name (str): Unique task name (e.g. MyTask).
            cache (Cache): A Cache object to store outputs.
            dependencies (list[str|DataSpec]): Names of Results (as produced by other Tasks)
                required by this Task.
            outputs (list[DataSpec]): DataSpec objects expected to be fulfilled.
            manual_execution_only (bool): Tells Pipelines not to run this task (default False).
            ...
        """
        self._name: str = name
        if not outputs:
            outputs = []
        if not all([isinstance(o, DataSpec) for o in outputs]):
            raise TypeError("Task outputs must be DataSpec objects")
        self._outputs = outputs

        # User-provided dependency names (technically DataSpec/Result names)
        self._input_dependencies: list[str | DataSpec] = dependencies or []

        self.if_upstream_errors = if_upstream_errors

        # Flags for pipeline
        self.manual_execution_only = manual_execution_only

        self._process_times = []
        self._executed = False
        self._skipped = False
        self._has_errors = False

        self._main_func: Callable | None = None

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
        self._pipeline: Pipeline | None = None
        self._raise_errors = True
        # Initialize the cache stuff ....
        self._cache: Cache = cache
        self.use_cached_results = use_cached_results
        self.result_expiry = result_expiry  # NOTE: None
        self._result_names = set()

        # Logging
        self.logger = get_logger()

    # ========================================================================
    # Properties

    @property
    def cache(self) -> Cache:
        if not self._cache:
            raise exc.CacheNotSetError("This Task has no Cache")
        return self._cache

    @cache.setter
    def cache(self, cache: Cache) -> None:
        self._cache = cache
        return

    @property
    def pipeline(self):
        """
        Reference to the Pipeline.
        """
        if not self._pipeline:
            raise exc.PipelineNotSetError("This Task has not be added to a Pipeline")
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: "Pipeline") -> None:
        self._pipeline = pipeline
        return

    @property
    def outputs(self) -> dict[str, type]:
        """
        Task outputs.
        """
        return {spec.name: spec for spec in self._outputs}

    @property
    def _script_hash(self):
        """
        SHA256 has of the Python file containing the Task.
        """
        try:
            return util.hashing.hash_file(self.script_path)
        except Exception:
            return ""

    @property
    def name(self) -> str:
        """
        Task name.
        """
        return self._name

    @property
    def is_executed(self) -> bool:
        """
        """
        return self._executed and not self._has_errors

    @property
    def is_skipped(self):
        """
        """
        return self._skipped

    @is_skipped.setter
    def is_skipped(self, value: bool):
        """
        """
        self._skipped = value

        if value is True:
            self.logger.warn(self.name, f"Skipped {value}")  # TODO: test
            # TODO: not sure what other callbacks are useful here
        
        return

    @property
    def has_main(self):
        return self._main_func is not None

    @property
    def dependency_names(self) -> set[str]:
        """
        Names of Results that this Task depends on (were created by upstram Tasks).
        """
        # Where i is either a str or DataSpec
        return {i.name if isinstance(i, DataSpec) else i for i in self._input_dependencies}

    @property
    def dependencies(self) -> dict[str, Result] | None:
        """
        Results created by upstream Tasks that are accessible to this Task.
        """
        results = {}
        for task_name in self.pipeline.get_upstream(self.name):
            for r in self.cache.get_for_task(task_name) or []:
                results[r.name] = r
        return DependencyDict(results)

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
        if not self.outputs:
            return

        actual = {result.name: result for result in results}
        expected_names = set(self.outputs.keys())
        actual_names = set(actual.keys())

        # ============================================================
        # Missing outputs

        for missing_name in sorted(expected_names - actual_names):
            raise exc.MissingOutputError(f"Missing output: '{missing_name}'")

        # ============================================================
        # Unexpected outputs

        for unexpected_name in sorted(actual_names - expected_names):
            raise exc.UnexpectedOutputError(f"Unexpected output: '{unexpected_name}'")

        # ============================================================
        # Duplicate names

        seen = set()
        duplicates = set()
        for r in results:
            if r.name in seen:
                duplicates.add(r.name)
            else:
                seen.add(r.name)
        if duplicates:
            raise exc.DuplicateResultsError(f"Multiple Results have the same name: {duplicates}")

        # ============================================================
        # Type validation + DataSpec validation

        for output_name in (expected_names & actual_names):
            result = actual[output_name]
            spec = self.outputs[output_name]
            spec.validate(result.value)

        return

    def get_results(self, named=False, allow_stale=False) -> list[Result] | dict[str, Result]:
        """
        Gets the Task's result(s).

        Args:
            named (bool): Returns a dict[str, Result] if true, list[Results] if False
            details (bool): Returns the result as a Result
            run_if_not_cached (bool): Runs the task if the results are not already cached and
                returns the results of that process
        """
        # if not allow_stale and not self.is_executed:  # TODO: wip
        #     raise Exception()
        # elif allow_stale and not self.is_executed:
        #     self.logger.warn(f"Results for {self.name} might be stale")

        results: list[Result] | dict[str, Result]
        results = self.cache.get_for_task(self.name)
        if named:
            results = {r.name: r for r in results}
        return results

    def _default_cache_read_callback(self, cached_result: Result) -> Any:
        """
        The default cache-read callback.
        """
        return cached_result.value

    def on_cache_read(self, func: Callable):
        """
        Decorator used to override the cache-read callback.
        """
        from types import MethodType
        self._cache_read_callback = MethodType(func, self)
        return func

    def cache_read_callback(self, cached_result: Result):
        """
        Allow a user-defined function to fire after reading data from the Cache.
        """
        return self._cache_read_callback(cached_result)

    def help(self) -> str:
        """
        Provides a self-documentating help string.
        """
        # See _autodoc.py for the basic concept
        raise NotImplementedError("Coming soon")

    # ========================================================================
    # Decorators

    def process(self, func: Callable) -> None:
        """
        Wrapper for method-like custom functions.
        """
        @wraps(func)
        def _process_wrapper(*args, **kwargs) -> Any:
            self.logger.process_start(self.name, func.__name__)
            with util.process_timer() as _t:
                with self.logger.spinner(f"{self.name}: Running {func.__name__}..."):
                    result = func(self, *args, **kwargs)
            return result

        setattr(self, func.__name__, _process_wrapper)
        return

    def _resultify(self, return_values: Any | tuple[Result]) -> tuple[Result]:
        """
        Forces the value(s) returned by 'main' into a tuple of Result(s).

        Arg:
            return_values (Any | tuple[Result]): The user's main function may return:
                - A single value (Any). It is wrapped in a Result named after the
                  Task's sole declared output if exactly one output is declared,
                  otherwise after the Task itself.
                - A single Result object, which inherits the task.name if it has none.
                - A non-empty list or tuple of Results, which are used as-is
                  (any missing task_name is filled in).

        Any other list or tuple (e.g. a plain list of numbers) is treated as a
        single value, not as a collection of Results. Mixing Results and
        non-Results in one list or tuple raises a TypeError.
        """
        # Nothing returned. Use isinstance rather than `== tuple()`, since
        # comparing arrays/DataFrames to a tuple yields an ambiguous truth value.
        is_empty_tuple = isinstance(return_values, tuple) and len(return_values) == 0
        if (return_values is None or is_empty_tuple) and self.outputs:
            raise exc.MissingOutputError(
                f"Nothing returned by Task('{self.name}').main()"
            )

        # Single Result
        if isinstance(return_values, Result):
            if not return_values.task_name:
                return_values.task_name = self.name
            results = (return_values,)

        # Empty tuple with no declared outputs: nothing to do
        elif is_empty_tuple:
            results = ()

        # List/tuple made up entirely of Results
        elif (
            isinstance(return_values, (list, tuple))
            and return_values
            and all(isinstance(item, Result) for item in return_values)
        ):
            for rt in return_values:
                if not rt.task_name:
                    rt.task_name = self.name
            results = tuple(return_values)

        # A mix of Results and other objects is almost certainly a mistake
        elif (
            isinstance(return_values, (list, tuple))
            and any(isinstance(item, Result) for item in return_values)
        ):
            raise TypeError(
                f"Task('{self.name}').main() returned a mix of Result and non-Result "
                "objects. Return either only Results or a single plain value."
            )

        # Anything else is a single value (including plain lists/tuples, arrays,
        # DataFrames, etc.)
        else:
            declared = list(self.outputs)
            result_name = declared[0] if len(declared) == 1 else self.name
            results = (
                Result(value=return_values, name=result_name, task_name=self.name),
            )

        # Duplicate names
        names = [r.name for r in results]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise exc.DuplicateResultsError(
                f"Multiple Results have the same name: {duplicates}"
            )

        return results

    def _cache_and_return_result_data(self, results: tuple[Result]) -> tuple[Any]:
        """
        Handles return values as Results and puts them in the Cache.
        """
        unpacked_data: list[Any] = []
        return_data: Any | tuple[Any]
        for result in results:
            try:
                self.cache.put(result)
            except Exception as e:
                self._has_errors = True
                self.logger.error(
                    task=self.name,
                    msg=f"Failed to cache Result ({type(result).__name__})"
                )
                self.logger.error(task=self.name, msg=f"{e.__class__.__name__}: {e}")
            unpacked_data.append(result.value)

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
            raise_errors (bool): 
        """
        if self._main_func:
            raise AttributeError(f"Multiple uses of `@Task({self.name}).main` decorator")

        @wraps(func)
        def _main_wrapper(*args, **kwargs) -> Any | tuple[Any]:
            """
            Little Pipelines' secret sauce.
            """
            kwargs_allowed = [
                "force",
                "raise_errors",
            ]
            self.logger.task_start(self.name)

            # Pop the control kwargs so they never reach the user's function
            force = kwargs.pop("force", True)
            raise_errors = kwargs.pop("raise_errors", True)
            if not isinstance(force, bool):
                raise TypeError(f"'force' kwarg must be bool: {force}")
            if not isinstance(raise_errors, bool):
                raise TypeError(f"'raise_errors' kwarg must be bool: {raise_errors}")
            self._raise_errors = raise_errors

            self._executed = False
            self._has_errors = False

            with util.process_timer() as _t:
                try:
                    # Use cached results when allowed
                    if not force and self.use_cached_results:
                        cached = self.cache.get_for_task(self.name)
                        if cached is not None:
                            self._executed = True
                            results = [self.cache_read_callback(r) for r in cached]
                            return results[0] if len(results) == 1 else tuple(results)

                    # Run, normalize, validate, store
                    return_values = func(self, *args, **kwargs)
                    results = self._resultify(return_values)
                    self._validate_outputs(results)
                    unpacked_data = self._cache_and_return_result_data(results)

                except Exception as e:
                    self._has_errors = True
                    self.logger.error(
                        task=self.name,
                        msg=f"{e.__class__.__name__}: {e}",
                    )
                    if raise_errors:
                        raise
                    return None  # Aborts the task; acts like a `continue`

            self._executed = True
            self.logger.task_complete(self.name, _t)
            return unpacked_data

        self._main_func = func
        self.main = _main_wrapper
        return

    # ========================================================================
    # Dunders

    def __repr__(self):
        return f"<Task ('{self.name}')>"


__all__ = ["find_tasks", "Task"]
