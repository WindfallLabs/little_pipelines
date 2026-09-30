"""
Pipeline - The orchestration.
"""

import importlib
import inspect
from collections import defaultdict
from collections.abc import Callable, Generator
from functools import cached_property
from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING, Any

from . import Cache, Result, exc, util
from .messaging import get_logger
from .pipeline_run import PipelineRun

if TYPE_CHECKING:
    from ._tasks import Task


class Pipeline:
    """
    Manages task orchestration and execution.
    """
    def __init__(
        self,
        name: str,
        expire_results_if_none: bool = True,
        cache: Cache | None = None,
    ):
        """
        Initialize a pipeline.

        Args:
            name (str): Pipeline name (names the cache's parent folder).
            expire_results_if_none (bool): Delete None results on pipeline complete.
            cache (Cache): A cache object.
        """
        self.name = name
        self.expire_results_if_none = expire_results_if_none
        self.cache: Cache = cache or Cache()

        self._tasks: list[Task] = []
        self.failures: set = set()
        self.execution_errors: list[Exception] = []
        self.previous_run: PipelineRun|None = None
        self.current_run: PipelineRun|None = None

        # Optional callback functions
        self._on_complete: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []
        self._on_fail: list[tuple[Callable, tuple[Any], dict[Any, Any]]] = []

        # The script the Pipeline is initialized in
        self._script = inspect.getmodule(inspect.currentframe().f_back)

        # Logging
        self.logger = get_logger()

    @property
    def is_complete(self) -> bool:
        """
        If all Pipeline Tasks have been completed.
        """
        return all([task.is_executed or task.is_skipped for task in self.tasks])

    @property
    def ntasks(self) -> int:
        """
        Task count.
        """
        return len(self._tasks)

    @property
    def dependency_graph(self) -> dict[str, list[str]]:
        """
        Unsorted dict of {task: [dependency]}
        """
        return {
            t.name: [self.get_task(i).name for i in t.dependency_names] for t in self._tasks
        }

    @cached_property
    def _reverse_graph(self) -> dict:
        reverse = defaultdict(list)
        for task, deps in self.dependency_graph.items():
            for dep in deps:
                reverse[dep].append(task)
        return reverse

    @property
    def topologically_sorted(self) -> Generator[str]:
        """
        Tasks, sorted by dependencies.
        """
        ts = TopologicalSorter(self.dependency_graph)
        sorted_tasks = ts.static_order()
        return sorted_tasks

    @property
    def tasks(self) -> Generator["Task"]:
        """
        Generates the execution order of tasks based on dependencies.
        """
        for task_name in self.topologically_sorted:
            yield self.get_task(task_name)

    def get_upstream(self, task_name: str) -> list[str]:
        """
        Return all upstream dependencies of 'task_name' in topological order.
        """
        order = list(self.topologically_sorted)

        visited = set()
        stack = list(self.dependency_graph.get(task_name, []))

        while stack:
            node = stack.pop()
            if node is None or node in visited:
                continue
            visited.add(node)
            stack.extend(self.dependency_graph.get(node, []))

        return [t for t in order if t in visited]

    def get_downstream(self, task_name: str) -> list[str]:
        """
        Return all tasks downstream of `task_name` and upstream of those.
        """
        if task_name not in self.dependency_graph:
            raise KeyError(f"Unknown task: {task_name}")

        visited = set()
        stack = [task_name]

        while stack:
            node = stack.pop()
            for dependent in self._reverse_graph.get(node, ()):
                if dependent not in visited:
                    visited.add(dependent)
                    stack.append(dependent)

        return [t for t in self.topologically_sorted if t in visited]

    def add(self, *tasks: "Task") -> "Pipeline":
        """
        Add Tasks to the Pipeline.
        """
        for task in tasks:
            # Relate the pipeline to the task
            task.pipeline = self
            self._tasks.append(task)
        return self

    def list_tasks(
        self,
        show_has_cached_data=False
    ) -> list[str] | list[tuple[str, bool, str, str]]:
        """
        Return a list of Task names, optionally showing the number of cached Results per task.
        """
        tasks: list[str] = [t.name for t in self.tasks]
        if not show_has_cached_data:
            return tasks

        task_list: list[tuple[str, bool]] = []
        for t in self.tasks:
            try:
                results: list[Result] = t.get_results()
                task_list.append((t.name, len(results)))
            except AttributeError:
                task_list.append((t.name, 0))
        return task_list

    def check_failed_dependencies(self, task: "Task") -> bool:
        """
        Checks if the Task's dependencies have failed.
        
        Raises DependencyFailure
        Returns Boolean
        """
        failed_deps = set(task.dependencies).intersection(self.failures)
        if failed_deps != set():
            return True
        return False

    def get_task(self, task_name: str):
        """
        Gets a task by name.
        """
        # Dict of task-name: Task
        task_lookup: dict[str, Task] = {task.name: task for task in self._tasks}
        # Add result-name: Task
        task_lookup.update({k: task for task in self._tasks for k in task.outputs.keys()})
        try:
            t = task_lookup[task_name]
            return t
        except KeyError as e:
            raise exc.DependencyNotFoundError(f"No such task: {task_name}") from e

    def reload_task(self, task_name: str|None = None) -> None:
        """
        Reload one or all tasks and then reload the pipeline script.
        """
        if task_name:
            task = self.get_task(task_name)
            importlib.reload(task._script)
        else:
            for task in self.tasks:
                importlib.reload(task._script)
        importlib.reload(self._script)
        return

    def reload(self) -> None:
        """
        Reloads the Pipeline and all Tasks.
        """
        self.reload_task()
        return

    def _validate_required_methods(self) -> None:
        """
        Ensure all Tasks define a main process.
        """
        missing_main = [
            task.name
            for task in self._tasks
            if not task.has_main
        ]

        if missing_main:
            raise exc.MissingMainProcessError(
                f"Tasks missing 'main' process: "
                f"{', '.join(sorted(missing_main))}"
            )

        return

    def _validate_duplicate_names(self) -> None:
        """
        Ensure Task names are unique.
        """
        task_names: list[str] = [task.name for task in self._tasks]
        name_set: set[str] = set(task_names)
        has_duplicates = len(task_names) != len(name_set)

        if has_duplicates:
            duplicates = {
                name
                for name in task_names
                if task_names.count(name) > 1
            }
            raise exc.DuplicateTaskError(
                f"Duplicate task names: {sorted(duplicates)}"
            )

        return

    def _validate_missing_dependencies(self) -> None:
        """
        Ensure all dependencies reference known Tasks or Results.
        """
        known_names = {
            task.name
            for task in self._tasks
        }
        known_names.update(
            {
                result_name
                for task in self._tasks
                for result_name in task.outputs.keys()
            }
        )
        missing: list[tuple[str, str]] = []
        for task in self._tasks:
            for dep in task.dependency_names:
                if dep not in known_names:
                    missing.append(
                        (task.name, dep)
                    )
        if missing:
            lines = [
                f"{task_name} -> {dep_name}"
                for task_name, dep_name in missing
            ]
            raise exc.MissingDependencyError(
                "Missing dependencies:\n" + "\n".join(lines)
            )

        return

    def _validate_cycles(self) -> None:
        """
        Ensure the Task dependency graph is acyclic.
        """
        graph: dict[str, list[str]] = {}
        for task in self._tasks:
            deps: list[str] = []
            for dep in task.dependency_names:
                # Result dependencies are not part of the task graph.
                try:
                    self.get_task(dep)
                    deps.append(dep)
                except KeyError:
                    continue
            graph[task.name] = deps

        try:
            ts = TopologicalSorter(graph)
            ts.prepare()
        except CycleError as e:
            raise exc.CircularDependencyError(
                f"Circular dependency detected: {e}"
            ) from e

    def validate_tasks(self) -> None:
        """
        Pre-flight validation checks.

        Collect all validation failures and raise them together.
        """

        errors: list[exc.PipelineValidationError] = []

        validators = (
            self._validate_required_methods,
            self._validate_duplicate_names,
            self._validate_missing_dependencies,
            self._validate_cycles,
        )

        for validator in validators:
            try:
                validator()
            except Exception as e:
                errors.append(e)

        if errors:
            raise ExceptionGroup(
                "Pipeline validation failed",
                errors,
            )

        return

    def execute(
        self,
        force_all = False,
        force_tasks: list[str] | None = None,
        skip_tasks: list[str] | None = None,
        raise_errors: bool = True,
        **kwargs
    ) -> None:
        """
        Execute the pipeline.

        Args:
            force_all (bool): Clears all previously cached results before execution.
            force_tasks (list[str]): Tasks to force execute.
            skip_tasks (list[str]): Tasks to skip/ignore during execution.
            raise_errors (bool): Errors will interupt execution (default True).
        """
        _timer = util.Timer().start()

        self.previous_run = self.current_run
        self.current_run = PipelineRun(self.name, _timer._start_dt)
        # Clear errors
        self.execution_errors = []

        if not force_tasks:
            force_tasks = []  # TODO: deprecate (set this at the task-level)
        if not skip_tasks:
            skip_tasks = []

        # Validate all tasks have run methods
        self.validate_tasks()

        # Extract tasks from generator
        tasks = list(self.tasks)
        self.current_run.tasks_total = len([t for t in tasks if not t.manual_execution_only])
        manual_tasks = len([t for t in tasks if t.manual_execution_only])

        # Set the logger's max task name spacing
        self.logger.set_max_task_name_len(max([len(t.name) for t in tasks]) + 4)

        # Print
        self.logger.info(
            task="Pipeline",
            msg=f"Executing {len(tasks)} Task(s)..."
        )

        for task in tasks:
            # Handle manual_execution_only tasks (i.e. are not executed by pipeline)
            if task.manual_execution_only is True:
                #task.is_skipped = True  # TODO: this makes sense right?
                continue
            if force_all or task.name in force_tasks:
                task.cache.clear(task.name)
            # Handle ignored tasks
            if task.name in skip_tasks and task.name not in force_tasks:
                self.logger.warn(task=task.name, msg="Skipped (by user)")
                task.is_skipped = True
                continue

            # ================================================================
            # Execute task

            try:
                # Handle if upstream tasks (dependencies) failed
                if self.check_failed_dependencies(task):  # Raises or returns bool
                    task.is_skipped = True
                    continue

                # Execute
                result: Any = task.main(raise_errors=raise_errors)

            except Exception as e:
                pass

            if task.has_errors:
                self.current_run.tasks_failed += 1
                self.failures.add(task.name)
                self.execution_errors.extend(task._errors)
            else:
                if result is None:
                    self.logger.warn(task=task.name, msg="Result is None")

                self.current_run.tasks_executed += 1

        # ====================================================================
        # Post Execution

        self.current_run.tasks_skipped = len([t for t in tasks if t.is_skipped is True])

        _timer.stop()
        self.logger.pipeline_complete(
            f"Ran {self.current_run.tasks_executed}/{self.current_run.tasks_total} "
            f"tasks in {_timer}"
        )

        if self.current_run.tasks_skipped > 0 or manual_tasks > 0:
            man_tasks = ""
            if manual_tasks > 0:
                man_tasks = f"(+{manual_tasks} manual-only)"
            self.logger.warn(
                msg=f"Skipped: {self.current_run.tasks_skipped}/{self.current_run.tasks_total} "
                f"tasks {man_tasks}"
            )
        if self.current_run.tasks_failed > 0:
            self.logger.error(
                msg=f"Failed: {self.current_run.tasks_failed}/{self.current_run.tasks_total} tasks"
            )
        self.logger.stop()

        # Cache the PipelineRun
        self.current_run.stop()
        if self.cache is not None:
            self.cache.put_run(self.current_run)

        return

    def execute_one(
        self,
        task_name: str,
        force: bool = False,
        upstream: bool = True,
        downstream: bool = True,
        **kwargs
    ):
        """
        Executes a single task (optionally including upstream and/or downstream tasks).

        Args:
            task_name (str): The name of the target task to execute
            force (bool): False will pull cached data; True forces execution
            upstream (bool): Execute upstream tasks (and their dependencies)
            downstream (bool): Execute downstream tasks (and their dependencies)
        """
        _timer = util.Timer().start()

        self.previous_run = self.current_run
        self.current_run = PipelineRun(self.name, _timer._start_dt)
        # Clear errors
        self.execution_errors = []

        try:
            # Get target task
            self.logger.pipeline_info("Preparing Target Task...")
            with util.process_timer() as _t:
                target_task = self.get_task(task_name)
                if force:
                    self.cache.clear(task_name)
            self.current_run.tasks_total += 1  # TODO: always just one?
            self.logger.pipeline_info(f"completed in {_t}")

            # Get upstream and downstream tasks
            self.logger.pipeline_info("Preparing Upstream and Downstream Tasks...")
            upstream_tasks = []
            downstream_tasks = []
            with util.process_timer() as _t:
                # Upstream
                if upstream:
                    upstream_tasks = self.get_upstream(task_name)
                    self.current_run.tasks_total += len(upstream_tasks)
                # Downstream
                if downstream:
                    downstream_tasks = self.get_downstream(task_name)
                    self.current_run.tasks_total += len(downstream_tasks)
            self.logger.pipeline_info(f"completed in {_t}")

            self.logger.pipeline_info("Executing task(s)...")
            with util.process_timer() as _t:
                # Upstream tasks
                for tname in upstream_tasks:
                    task = self.get_task(tname)
                    if force:
                        self.cache.clear(tname)
                    task.main()
                    if task.is_executed:
                        self.current_run.tasks_executed += 1
                    else:
                        self.current_run.tasks_failed += 1
                # Target task
                target_task.main(**kwargs)
                if target_task.is_executed:
                    self.current_run.tasks_executed += 1
                else:
                    self.current_run.tasks_failed += 1
                # TODO: why bother with downstream processes if this fails?
                # elif target_task.has_errors:
                #     raise Exception("Error")

                # Downstream
                for tname in downstream_tasks:
                    task = self.get_task(tname)
                    if force:
                        self.cache.clear(tname)
                    task.main()
                    if task.is_executed:
                        self.current_run.tasks_executed += 1
                    else:
                        self.current_run.tasks_failed += 1
            self.logger.pipeline_info("Task(s) completed")

            _timer.stop()
            self.logger.pipeline_complete(
                f"Ran {self.current_run.tasks_executed}/{self.current_run.tasks_total} "
                f"tasks in {_timer}"
            )

        except Exception as e:
            self.execution_errors.append(e)
            # raise e
        # TODO: Log full stack?
        #finally:
        #    if raise_errors:
        #        raise ExceptionGroup("Multiple errors occurred", self.execution_errors)

        # Cache the PipelineRun
        self.current_run.stop()
        if self.cache is not None:
            self.cache.put_run(self.current_run)

        self.logger.stop()

        return

    def __repr__(self):
        return f"<Pipeline: {self.name} ({self.ntasks} tasks)>"


__all__ = ["Pipeline"]
