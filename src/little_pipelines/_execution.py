"""Execution"""

from graphlib import TopologicalSorter
from typing import Any, Generator
from warnings import warn

from ._logger import app_logger
from ._tasks import Task


def validate_tasks():
    """Pre-flight checks."""
    run_errors: list[str] = []
    for task in Task.registry:
        # Check if task has run method (required)
        if not hasattr(task, "run"):
            run_errors.append(task.name)

        # Check if dependencies are set to manual
        if task._execution_type == "MANUAL":
            if Task.show_warnings:  # TODO: config?
                for dependent in task.dependents:  # TODO: add log.warn
                    app_logger.warning(f"'{dependent.name}' task may fail due to manual execution of '{task.name}'")

    if run_errors:
        raise AttributeError(
            f"Tasks missing 'run' method: {', '.join(run_errors)}"
        )
    return


def execution_order(ignore_manual=True) -> Generator[Task]:
    """Generates the order of tasks based on dependencies."""
    deps: dict[str, list[str]] = {
        dep.name: dep._dependency_names for dep in Task.registry
    }
    sorted_names = TopologicalSorter(deps).static_order()
    for cls_name in sorted_names:
        task = Task.get_task(cls_name)
        # Ignore manually-executed tasks
        if task._execution_type == "MANUAL" and ignore_manual:
            continue
        yield task


# def execute_all():
#     """Executes all tasks."""
#     for task in execution_order():
#         app_logger.log("APP", f"====== Executing {task.name} ======")
#         task.run()
#     return


def execute_task(task_name: str, downsteam=False):
    """Executes a task by name."""
    task = Task.get_task(task_name)
    task.run()
    # TODO: Not sure this is smart...
    #if downsteam:
    #    for dependent in task.dependencies:
    #        dependent.run()
    return


def get_task_data(task_name: str, key: str):
    """Get data from the given task's datastore."""
    data: Any = Task.get_task(task_name).datastore.get(key)
    # Warn about no data
    if data is None:  # TODO: add log.warn
        app_logger.warning(f"Datastore value not found: '{key}' of task '{task_name}'")
    return data
