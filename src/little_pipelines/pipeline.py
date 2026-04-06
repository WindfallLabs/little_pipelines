"""
Pipeline
"""

# BUG: weird bug, if this code content changes, my dependent pipeline re-executes as if I cleared the cache...

from graphlib import TopologicalSorter
from pathlib import Path
from time import perf_counter_ns
from types import ModuleType
from typing import Any, Callable, Optional, Generator, TYPE_CHECKING

from . import _messages as msg
from . import util
#from .caching import Cache, CacheResult
from ._exceptions import DependencyFailure, PipelineValidationError
#from ._logger import app_logger

if TYPE_CHECKING:
    from ._tasks import Task


class Pipeline:
    """
    Manages task execution..
    """

    def __init__(
        self,
        name: str,
        expire_results_if_none: bool = True,
        #cache: Optional[Cache] = None,
    ):
        """
        Initialize a pipeline.

        Args:
            name (str): Pipeline name (names the cache's parent folder)
            expire_results_if_none (bool): Delete None results on pipeline complete
        """
        self.name = name
        self.expire_results_if_none = expire_results_if_none
        #self.cache = cache  # TODO: more

        self._tasks: list["Task"] = []
        self.failures: set = set()

        # Cache
        #self.cache: Cache = default_cache if cache is None else cache
        #self.cache: Optional[Cache] = cache if cache else None
        #self.logger: _Logger = build_logger(name)
        #self._log_dir: Optional[Path] = None

        # Optional callback functions
        self._on_complete: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []
        self._on_fail: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []

    @property
    def is_complete(self) -> bool:
        """If all Pipeline Tasks have been completed."""
        return all([task.is_executed or task.is_skipped for task in self.tasks])

    # @property
    # def log_dir(self):
    #     """Pipeline-specific log directory."""
    #     if self._log_dir:
    #         return self._log_dir
    #     return

    @property
    def ntasks(self) -> int:
        """Task count."""
        return len(self._tasks)

    #@cached_property
    # @property
    # def _max_task_name_len(self) -> int:
    #     """Names of all tasks."""
    #     return 1 + max([len(t.name) for t in self.tasks])

    #@cached_property
    @property
    def message(self) -> msg.Message:
        """Handle writing to the console."""
        msg_len = 1 + max([len(t.name) for t in self.tasks])
        return msg.Message(msg_len)

    @property
    def tasks(self) -> Generator["Task"]:
        """Generates the execution order of tasks based on dependencies."""
        deps: dict[str, list[str]] = {
            dep.name: dep._dependency_names for dep in self._tasks
        }
        for cls_name in TopologicalSorter(deps).static_order():
            task: "Task" = self.get_task(cls_name)
            yield task

    def add(self, *tasks: "Task") -> None:
        """Add Tasks to the Pipeline."""
        for task in tasks:
            task.pipeline = self
            self._tasks.append(task)
        return

    def list_tasks(self, show_has_cached_data=False) -> list[str] | list[tuple[str, bool]]:
        """Returns a list of task names, optionally with whether or not they have cached results."""
        tasks: list[str] = [t.name for t in self.tasks]
        if not show_has_cached_data:
            return tasks

        task_list: list[tuple[str, bool]] = []
        for t in self.tasks:
            task_list.append((t.name, t.name in t.cache.keys()))
        return task_list

    def check_failed_dependencies(self, task: "Task") -> bool:
        """Checks if the Task's dependencies have failed.
        
        Raises DependencyFailure
        Returns Boolean
        """
        failed_deps = set(task.dependencies).intersection(self.failures)
        if failed_deps != set():
            msg = f"Failed dependencies: {failed_deps}"
            #if task.if_upstream_errors == "FAIL":
            #    raise DependencyFailure(msg)
            #else:
            #    task.logger.warning(msg)
            return True
        return False

    def get_task(self, task_name: str):
        """Gets a task by name."""
        task_lookup: dict[str, "Task"] = {task.name: task for task in self._tasks}
        return task_lookup[task_name]  # TODO: We want this to error if need be

    # def get_result(self, task_name: str, details=False) -> Any:
    #     """
    #     Gets a Task's result from the cache.
    #     Raises KeyError if result or task doesn't exist.
    #     """
    #     # Return cached data if exists
    #     try:
    #         result: CacheResult | None = self.cache.get(task_name)
    #         if details:
    #             return result
    #         #if result:  # TODO: and not result.is_expired()
    #         #    return result.data
    #         return result.data
    #     except KeyError:
    #         pass  # Continue to next try-block

    #     # Run the task and return the result
    #     try:
    #         task: "Task" = self.get_task(task_name)
    #         result: Any = task.run()
    #         return result
    #     except KeyError:
    #         raise KeyError(f"No such task: '{task_name}'")
    #     except AttributeError:
    #         raise AttributeError("Task is not associated with a Pipeline")

    def reload_tasks(self, task_name: Optional[str] = ""):
        """Reloads task source code to apply any changes to task source code to the pipeline."""
        import importlib.util
        from little_pipelines import Task

        for task in self.tasks:
            if task_name and task.name != task_name:
                continue  # Skip until named task is found, if named task
            # Get the script containing the task's source code / definition as a module
            spec = importlib.util.spec_from_file_location(
                Path(task._script_path).name.strip(".py"),
                task._script_path
            )
            module = importlib.util.module_from_spec(spec)
            # Re-import
            spec.loader.exec_module(module)
            # Rebuild the task on the pipeline
            idx = self._tasks.index(task)  # Index of task on the pipeline
            # Get the variable name in the module
            task_var_name = [k for k, v in vars(module).items() if isinstance(v, Task)][0]
            # Set the 'new' task on the pipeline
            self._tasks[idx] = getattr(module, task_var_name)

        return

    def validate_tasks(self):
        """Pre-flight checks."""
        run_errors: list[str] = []

        # Check if task has run method (required)
        for task in self._tasks:  # Unsorted
            if not hasattr(task, "run"):
                run_errors.append(task.name)

        # Check if task dependencies are imported
        # TODO: improve this so that the error returns a list of all invalid deps
        try:
            list(self.tasks)  # TODO: this is a shorthand workaround for now
        except KeyError as e:
            raise PipelineValidationError(f"Missing dependency for TODO: {e}")  # TODO: task name

        # TODO: More checks?

        if run_errors:
            raise AttributeError(
                f"Tasks missing 'run' process: {', '.join(run_errors)}"
            )
        return

    # def _cache_result(self, task: "Task", result: Any):
    #     """Caches task info and results."""
    #     # Cache the results
    #     self.cache.set(
    #         task.name,
    #         result,
    #         #expire=task.expire_results(),
    #         tag="RESULTS"
    #     )
    #     # Cache hashes
    #     self.cache.set(
    #         task.name + "_hashes",
    #         {
    #             "script": task._script_hash,
    #             "inputs": task._inputs_hash,
    #         },
    #         tag="HASHES"
    #     )
    #     return

    def execute(
        self,
        force_all = False,
        force_tasks: Optional[list[str]] = None,
        skip_tasks: Optional[list[str]] = None,
        single_task: str = "",
    ) -> None:
        """
        Execute the pipeline.

        Args:
            force_all (bool): Clears all previously cached results before execution
        """
        _start = perf_counter_ns()

        if not force_tasks:
            force_tasks = []
        if not skip_tasks:
            skip_tasks = []
        nexec = 0
        nfail = 0

        # Validate all tasks have run methods
        self.validate_tasks()

        # Extract tasks from generator
        tasks = list(self.tasks)
        ntasks = len(tasks)

        # TODO: Support execution of only one task, optionally without executing upstream dependencies
        if single_task:
            tasks = [t for t in self.tasks if t.name == single_task]
            # TODO: upstream deps

        for task in tasks:
            # Handle manual_execution_only tasks (i.e. are not executed by pipeline)
            if task.manual_execution_only is True:
                continue
            if force_all or task.name in force_tasks:
                task.cache.clear(task.name)
            # Handle ignored tasks
            if task.name in skip_tasks and task.name not in force_tasks:
                self.message.write(task.name, "Skipped (by user)", **msg.WARN)
                task.is_skipped = True
                continue

            # ================================================================
            # Execute task

            try:
                # Handle if upstream tasks (dependencies) failed
                if self.check_failed_dependencies(task):  # Raises or returns bool
                    task.is_skipped = True
                    #self.message.write(task.name, "Task ...?", **msg.WARN)
                    continue

                # Execute
                result: Any = task.run()
                if result is None:
                    self.message.write(task.name, "Result is None", **msg.WARN)
                nexec += 1

            except Exception as e:
                self.failures.add(task.name)
                self.message.write(task.name, e, **msg.FAIL)
                # TODO: Log full stack
                nfail += 1

        # ====================================================================
        # Post Execution
        nskip = len([t for t in tasks if t.is_skipped is True])
        # TODO: change nexec to be the same?
        nexec = nexec - nskip

        self.message.console.rule()
        _time = util.time_diff(_start, perf_counter_ns())
        self.message.write("Pipeline Completed", f"Ran {nexec}/{ntasks} tasks in {_time}", **msg.PIPELINE_COMPLETE)

        if nskip > 0:
            self.message.write(msg=f"Skipped: {nskip}/{ntasks} tasks", **msg.WARN)
        if nfail > 0:
            self.message.write(msg=f"Failed: {nfail}/{ntasks} tasks", **msg.FAIL)
        self.message.console.rule()

        return

    def __repr__(self):
        return f"<Pipeline: {self.name} ({self.ntasks} tasks)>"


__all__ = ["Pipeline"]
