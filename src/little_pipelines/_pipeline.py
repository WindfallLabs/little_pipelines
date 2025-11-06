"""Pipeline execution with checkpointing."""

from functools import cached_property
from graphlib import TopologicalSorter
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Callable, Optional, Generator, TYPE_CHECKING

from . import util
from . import expire
from ._cache import get_cache
from ._exceptions import DependencyFailure
from ._logger import app_logger

if TYPE_CHECKING:
    from diskcache import Cache
    from ._tasks import Task


class Pipeline:
    """
    Manages task execution..
    """

    def __init__(
        self,
        name: str,
        expire_results_if_none: bool = True,
    ):
        """
        Initialize a pipeline.

        Args:
            name (str): Pipeline name (names the cache's parent folder)
            expire_results_if_none (bool): Delete None results on pipeline complete
        """
        self.name = name
        self.expire_results_if_none = expire_results_if_none

        self._tasks: list["Task"] = []
        self.failures: set = set()

        # Mount the cache
        self.cache: "Cache" = get_cache(name)

        self._log_dir: Optional[Path] = None
        self._shell: Optional[str] = None

        # Optional callback functions
        self._on_complete: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []
        self._on_fail: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []

    @property
    def is_complete(self) -> bool:
        """If all Pipeline Tasks have been completed."""
        return all([task.is_executed or task.is_skipped for task in self.tasks])

    @property
    def log_dir(self):
        """Pipeline-specific log directory."""
        if self._log_dir:
            return self._log_dir
        return self.cache.directory

    @property
    def ntasks(self) -> int:
        """Task count."""
        return len(self._tasks)

    @cached_property
    def _max_task_name_len(self) -> int:
        """Names of all tasks."""
        return 1 + max([len(t.name) for t in self.tasks])

    @property
    def tasks(self) -> Generator["Task"]:
        """Generates the execution order of tasks based on dependencies."""
        deps: dict[str, list[str]] = {
            dep.name: dep._dependency_names for dep in self._tasks
        }
        for cls_name in TopologicalSorter(deps).static_order():
            task = self.get_task(cls_name)
            yield task

    def add(self, *tasks: "Task") -> None:
        """Add Tasks to the Pipeline."""
        for task in tasks:
            task.pipeline = self
            self._tasks.append(task)
        return

    def check_failed_dependencies(self, task: "Task") -> bool:
        """Checks if the Task's dependencies have failed.
        
        Raises DependencyFailure
        Returns Boolean
        """
        failed_deps = set(task.dependencies).intersection(self.failures)
        if failed_deps != set():
            msg = f"Failed dependencies: {failed_deps}"
            if task.if_upstream_errors == "FAIL":
                raise DependencyFailure(msg)
            else:
                task.logger.warning(msg)
            return True
        return False

    def get_task(self, task_name: str):
        """Gets a task by name."""
        task_lookup: dict[str, "Task"] = {task.name: task for task in self._tasks}
        return task_lookup[task_name]  # We want this to error if need be

    def get_result(self, task_name: str):
        """Gets a Task's result from the cache."""
        return self.cache.get(task_name)

    def validate_tasks(self):
        """Pre-flight checks."""
        run_errors: list[str] = []
        for task in self._tasks:  # Unsorted
            # Check if task has run method (required)
            if not hasattr(task, "run"):
                run_errors.append(task.name)

        # TODO: More checks?

        if run_errors:
            raise AttributeError(
                f"Tasks missing 'run' process: {', '.join(run_errors)}"
            )
        return

    def _cache_result(self, task: "Task", result: Any):
        """Caches task info and results."""
        # Cache the results
        self.cache.set(
            task.name,
            result,
            expire=task.expire_results(),
            tag="RESULTS"
        )
        # Cache hashes
        self.cache.set(
            task.name + "_hashes",
            {
                "script": task._script_hash,
                "inputs": task._inputs_hash,
            },
            tag="HASHES"
        )
        return

    def execute(
        self,
        force = False,
        force_tasks: Optional[list[str]] = None,
        skip_tasks: Optional[list[str]] = None,
    ) -> None:
        """
        Execute the pipeline.

        Args:
            force (bool): Clears all previously cached results before execution
        """
        _start = perf_counter_ns()

        if not force_tasks:
            force_tasks = []
        if not skip_tasks:
            skip_tasks = []
        nexec = 0
        nskip = 0
        nfail = 0

        # Validate all tasks have run methods
        self.validate_tasks()

        # Force - clears all results from cache
        if force:
            app_logger.info("Clearing cache (force=True)")
            self.cache.evict("RESULTS")

        # Execute tasks in order
        #app_logger.log("APP", f"Executing pipeline '{self.name}'...")
        app_logger.log("APP", f"{'Starting pipeline...'.ljust(self._max_task_name_len)}: EXEC : '{self.name}'")
        app_logger.debug(f"Shell: {self._shell}")

        tasks = list(self.tasks)
        for task in tasks:
            #task._executed = False  # Reset
            #task._skipped = False  # Reset
            task_log_base = f"{task.name.ljust(self._max_task_name_len)}:"

            # Handle ignored tasks
            if task.name in skip_tasks and task.name not in force_tasks:
                task.logger.warning(f"{task_log_base} SKIP : Skipped per user")
                task.is_skipped = True
                nskip += 1
                continue

            # Try to use cached results
            cached_hashes = self.cache.get(task.name + "_hashes", default=dict())
            has_same_script = (cached_hashes.get("script") == task._script_hash)
            has_same_inputs = (cached_hashes.get("inputs") == task._inputs_hash)
            task.logger.debug(
                f"{task.name} Script/Inputs Changed: {not has_same_script}/{not has_same_inputs}"
            )
            # Will be None if force=True
            cached_results = self.cache.get(task.name)

            if (cached_results is not None and has_same_inputs and has_same_script):
                task.logger.warning(
                    f"{task_log_base} SKIP : Using cached results ({type(task.result).__name__})"
                )
                task.is_skipped = True
                nskip += 1
                continue

            # ================================================================
            # Execute task

            try:
                task.logger.info(
                    f"{task_log_base} EXEC : Running..."
                )
                # Handle if upstream tasks (dependencies) failed
                if self.check_failed_dependencies(task):  # Raises or returns bool
                    task.is_skipped = True
                    nskip += 1
                    continue

                # Execute
                result = task.run()
                self._cache_result(task, result)
                nexec += 1

            except Exception as e:
                self.failures.add(task.name)
                task.logger.error(
                    f"{task_log_base} FAIL : {e}"
                )
                nfail += 1

        # ====================================================================
        # Post Execution
        #nexec = len([t for t in tasks if t.is_executed is True])
        #nskipped = len([t for t in tasks if t.is_skipped is True])
        #nfailed = len(self.failures)

        _time = util.time_diff(_start, perf_counter_ns())
        app_logger.success(f"Pipeline complete!   : DONE : <light-black>Ran {nexec}/{len(tasks)} tasks in {_time}</>")

        if nskip > 0:
            app_logger.warning(f"{''.ljust(self._max_task_name_len)}: INFO : Skipped {nskip}")
        if nfail > 0:
            app_logger.error(f"{''.ljust(self._max_task_name_len)}: INFO : Failed {nfail}")

        # Clean up - Handle on-complete expirations
        for key in expire._on_complete_deletions:
            app_logger.info(f"<light-black>Expiring results for {key}</>")
            self.cache.delete(key)
        # Clean up - Delete None results
        if self.expire_results_if_none:
            for key in self.cache.iterkeys():
                if self.cache[key] is None:
                    self.cache.delete(key)

        return

    def __repr__(self):
        return f"<Pipeline: {self.name} ({self.ntasks} tasks)>"


__all__ = ["Pipeline"]
