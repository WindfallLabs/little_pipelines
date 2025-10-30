"""Pipeline execution with checkpointing."""

from graphlib import TopologicalSorter
from pathlib import Path
from typing import Optional, Generator

from ._cache import get_cache
from ._logger import app_logger
from ._tasks import Task


class Pipeline:
    """
    Manages task execution..
    """

    def __init__(
        self,
        name: str,
        cache_name: Optional[Path] = None
    ):
        """
        Initialize a pipeline.

        Args:
            name: Pipeline name (used for cache directory)
            cache_dir: Custom cache directory (default: %USER%/.little_pipelines/{name})
        """
        self.name = name
        self.cache = get_cache(cache_name)
        self._tasks: list[Task] = []
        self.ignored_tasks: list[str] = []
        self.forced_tasks: list[str] = []

    @property
    def is_complete(self) -> bool:
        return all([task.is_executed for task in self.tasks])

    @property
    def ntasks(self):
        """The task count."""
        return len(self._tasks)

    @property
    def tasks(self) -> Generator[Task]:
        """Generates the execution order of tasks based on dependencies."""
        deps: dict[str, list[str]] = {
            dep.name: dep._dependency_names for dep in self._tasks
        }
        for cls_name in TopologicalSorter(deps).static_order():
            task = self.get_task(cls_name)
            yield task

    def add(self, *tasks: Task) -> None:
        """Add Tasks to the Pipeline."""
        self._tasks.extend(tasks)
        return

    def set_forced(self, *forced_tasks: str) -> None:
        """Task names for forced execution."""
        self.forced_tasks.extend(forced_tasks)
        return

    def clear_forced(self):
        """Clears forced tasks."""
        self.forced_tasks = []
        return

    def set_ignored(self, *ignored_tasks: str) -> None:
        """Task names to ignore during execution."""
        self.ignored_tasks.extend(ignored_tasks)
        return

    def clear_ignored(self):
        """Clears ignored tasks."""
        self.ignored_tasks = []
        return

    def get_task(self, task_name: str):
        """Gets a task by name."""
        task_lookup: dict[str, Task] = {task.name: task for task in self._tasks}
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
                f"Tasks missing 'run' method: {', '.join(run_errors)}"
            )
        return

    def execute(
        self,
        force = False,
    ) -> None:
        """
        Execute the pipeline.

        Args:
            force (bool): Clears all previously cached results before execution
        """

        # Validate all tasks have run methods
        self.validate_tasks()

        # Force - clears all results from cache
        if force:
            app_logger.info("Clearing cache (force=True)")
            self.cache.evict("RESULTS")

        # Execute tasks in order
        app_logger.log("APP", "Starting pipeline execution...")

        for task in self.tasks:
            task.pipeline = self  # TODO: why again?

            # Handle ignored tasks
            if task.name in self.ignored_tasks and task.name not in self.forced_tasks:
                app_logger.info(f"Ignoring: {task.name}")
                task.skipped = True
                continue

            # Try to use cached results
            cached_hashes = self.cache.get(task.name + "_hashes", default=dict())
            has_same_script = (cached_hashes.get("script") == task._script_hash)
            has_same_inputs = (cached_hashes.get("inputs") == task._inputs_hash)
            # Will be None if force=True
            cached_results = self.cache.get(task.name)

            if (cached_results is not None and has_same_inputs and has_same_script):
                app_logger.info(f"Using previous data: {task.name}")
                task.skipped = True
                continue

            # Execute task
            app_logger.info(f"Executing: {task.name}")
            result = task.run()

            # Cache the results
            self.cache.set(
                task.name,
                result,
                expire=task.expiry(),
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

        app_logger.success("Pipeline complete!")

    def __repr__(self):
        return f"<Pipeline: {self.name} ({self.ntasks} tasks)>"


__all__ = ["Pipeline"]
