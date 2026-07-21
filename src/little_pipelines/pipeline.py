"""
Pipeline
"""

# BUG: weird bug, if this code content changes, my dependent pipeline re-executes as if I cleared the cache...

from graphlib import TopologicalSorter
from time import perf_counter_ns
from typing import Any, Callable, Optional, Generator, TYPE_CHECKING

from . import _messages as msg
from . import Cache
from . import util
from .exc import PipelineValidationError
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
        cache=None,
    ):
        """
        Initialize a pipeline.

        Args:
            name (str): Pipeline name (names the cache's parent folder)
            expire_results_if_none (bool): Delete None results on pipeline complete
        """
        self.name = name
        self.expire_results_if_none = expire_results_if_none
        self.cache: Cache = cache  # TODO: maybe

        self._tasks: list["Task"] = []
        self.failures: set = set()
        self._quiet = True  # Default True here, shell defaults to False

        # Registry of task:dependencies
        self._task_deps: dict[str, list[str]] = {}

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

    @property
    def message(self) -> msg.Message:
        """Handle writing to the console."""
        lens = [6]  # Default minimum; no need to expose to users
        for t in self.tasks:
            try:
                lens.append(len(t.name))
            except Exception as e:  # KeyError, but why not everything
                pass
        msg_len = max(lens)
        return msg.Message(msg_len, self._quiet)

    @property
    def tasks(self) -> Generator["Task"]:
        """Generates the execution order of tasks based on dependencies."""
        if not self._task_deps:
            for task in self._tasks:
                self._task_deps[task.name] = []
                for dep_name in task._dependency_names:
                    try:
                        # Find Task-dependencies
                        _ = self.get_task(dep_name)
                        self._task_deps[task.name].append(dep_name)
                    except KeyError:
                        # Assume non-task dependencies are Result-dependencies
                        continue
        for task_name in TopologicalSorter(self._task_deps).static_order():
            task: "Task" = self.get_task(task_name)
            yield task

    def get_upstream_tasks(self, task_name: str) -> list[str]:
        """Return all upstream dependencies of `key` in topological order."""
        if not self._task_deps:
            _ = list(self.tasks)
        ts = TopologicalSorter(self._task_deps)
        order = list(ts.static_order())

        visited = set()
        stack = list(self._task_deps.get(task_name, []))

        while stack:
            node = stack.pop()
            if node is None or node in visited:
                continue
            visited.add(node)
            stack.extend(self._task_deps.get(node, []))

        return [t for t in order if t in visited]

    def get_downstream_tasks(self, task_name: str) -> list[str]:
        """Return all tasks downstream of `task_name` and upstream of those."""
        if not self._task_deps:
            _ = list(self.tasks)

        # Build a reverse graph: each node points to whoever depends on it
        reverse: dict[str, list[str]] = {k: [] for k in self._task_deps}
        for task, deps in self._task_deps.items():
            for dep in deps:
                if dep is not None:
                    reverse.setdefault(dep, []).append(task)

        ts = TopologicalSorter(self._task_deps)
        order = list(ts.static_order())

        visited = set()
        stack = list(reverse.get(task_name, []))

        while stack:
            node = stack.pop()
            if node == task_name or node in visited:
                continue
            visited.add(node)
            # keep walking downstream...
            stack.extend(reverse.get(node, []))
            # ...and pull in whatever this downstream task itself depends on
            stack.extend(self._task_deps.get(node, []))

        return [t for t in order if t in visited]

    def add(self, *tasks: "Task") -> None:
        """Add Tasks to the Pipeline."""
        for task in tasks:
            # Relate the pipeline to the task
            task.pipeline = self
            self._tasks.append(task)
        return

    def list_tasks(self, show_has_cached_data=False) -> list[str] | list[tuple[str, bool, str, str]]:
        """Returns a list of task names, optionally with whether or not they have cached results."""
        tasks: list[str] = [t.name for t in self.tasks]
        if not show_has_cached_data:
            return tasks

        task_list: list[tuple[str, bool]] = []
        for t in self.tasks:
            reason = ""
            try:
                result = t.get_result(True)
                last_updated = result.last_updated.strftime("%Y-%m-%d")
                task_list.append((t.name, True, last_updated, reason))
            except (AttributeError, KeyError, ValueError):
                # Task has no cache attr set
                if getattr(t, "cache", None) is None:
                    reason = "Cache not enabled"
                # No data in cache
                else:
                    reason = "No data"
                task_list.append((t.name, False, "", reason))
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
        try:
            t = task_lookup[task_name]
            return t
        except KeyError:
            raise KeyError(f"No such task: {task_name}")

    def validate_tasks(self):
        """Pre-flight checks."""
        run_errors: list[str] = []

        # Check if task has main or run method (required)
        for task in self._tasks:  # Unsorted
            if not hasattr(task, "main") and not hasattr(task, "run"):  # TODO: deprecate 'run'
                run_errors.append(task.name)

        # Check if task dependencies are imported
        # TODO: improve this so that the error returns a list of all invalid deps
        # try:
        #     list(self.tasks)  # TODO: this is a shorthand workaround for now
        # except KeyError as e:
        #     raise PipelineValidationError(f"Missing dependency for {task.name}: {e}")  # TODO: task name
        # Removed to allow access to non-task cached Results

        # TODO: More checks?

        if run_errors:
            raise AttributeError(
                f"Tasks missing 'run' process: {', '.join(run_errors)}"
            )
        return

    def execute(
        self,
        force_all = False,
        force_tasks: Optional[list[str]] = None,
        skip_tasks: Optional[list[str]] = None,
        quiet: bool = True,
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

        # Silence messages # TODO: WIP
        self._quiet = quiet

        # Validate all tasks have run methods
        self.validate_tasks()

        # Extract tasks from generator
        tasks = list(self.tasks)
        ntasks = len([t for t in tasks if not t.manual_execution_only])
        manual_tasks = len([t for t in tasks if t.manual_execution_only])

        for task in tasks:
            # Handle manual_execution_only tasks (i.e. are not executed by pipeline)
            if task.manual_execution_only is True:
                #task.is_skipped = True  # TODO: this makes sense right?
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
                if hasattr(task, "run"):  # TODO: deprecate
                    result: Any = task.run()
                else:
                    result: Any = task.main()
                if result is None:
                    self.message.write(task.name, "Result is None", **msg.WARN)
                nexec += 1

            except Exception as e:
                self.failures.add(task.name)
                self.message.write(task.name, f"{e.__class__.__name__}: {e}", **msg.FAIL)
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

        if nskip > 0 or manual_tasks > 0:
            man_tasks = ""
            if manual_tasks > 0:
                man_tasks = f"(+{manual_tasks} manual-only)"
            self.message.write(msg=f"Skipped: {nskip}/{ntasks} tasks {man_tasks}", **msg.WARN)
        if nfail > 0:
            self.message.write(msg=f"Failed: {nfail}/{ntasks} tasks", **msg.FAIL)
        self.message.console.rule()

        return

    def execute_one(
        self,
        task_name: str,
        force: bool = False,
        upstream: bool = True,
        downstream: bool = True,
        quiet: bool = True,
        **kwargs
    ):
        """
        Executes a single task (optionally including upstream and/or downstream tasks).

        Args:
            task_name (str): The name of the target task to execute
            force (bool): False will pull cached data; True forces execution
            upstream (bool): Execute upstream tasks (and their dependencies)
            downstream (bool): Execute downstream tasks (and their dependencies)
            quiet (bool): Silences printed messages (default False)
        """
        # Silence messages # TODO: WIP
        self._quiet = quiet

        target_task = self.get_task(task_name)
        if force:
            self.cache.clear(task_name)

        upstream_tasks = []
        if upstream:
            upstream_tasks = self.get_upstream_tasks(task_name)
        
        downstream_tasks = []
        if downstream:
            downstream_tasks = self.get_downstream_tasks(task_name)

        for tname in upstream_tasks:
            task = self.get_task(tname)
            if force:
                self.cache.clear(tname)
            task.main()
        
        target_task.main(**kwargs)

        for tname in downstream_tasks:
            task = self.get_task(tname)
            if force:
                self.cache.clear(tname)
            task.main()

        # TODO: make it look like the printed results of `execute`

        return

    def __repr__(self):
        return f"<Pipeline: {self.name} ({self.ntasks} tasks)>"


__all__ = ["Pipeline"]
