# tests/test_shell_validation.py

from unittest.mock import Mock

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Pipeline validation
# ============================================================================


def test_validate_runs_pipeline_validation(
    shell,
    mock_pipeline,
):
    shell.do_validate()

    mock_pipeline.validate_tasks.assert_called_once_with()


def test_validate_logs_start_and_finish(shell):
    shell.do_validate()

    shell.logger.shell_info.assert_any_call("Validating...")

    shell.logger.shell_info.assert_any_call("Validation completed.")


# ============================================================================
# Task validation
# ============================================================================


def test_validate_task_requires_name(shell):
    shell.do_validate_task("")

    shell.logger.shell_fail.assert_called_once_with("Task name required.")


def test_validate_task_valid(
    shell,
    mock_pipeline,
    task_factory,
):
    task = task_factory(
        name="BuildRoutes",
        has_main=True,
    )

    mock_pipeline.get_task.return_value = task

    shell.do_validate_task("BuildRoutes")

    mock_pipeline.get_task.assert_called_once_with("BuildRoutes")

    shell.console.print.assert_any_call("Task: BuildRoutes")

    shell.console.print.assert_any_call("[green]Valid.[/]")


def test_validate_task_missing_main_raises(
    shell,
    mock_pipeline,
    task_factory,
):
    task = task_factory(
        name="BuildRoutes",
        has_main=False,
    )

    mock_pipeline.get_task.return_value = task

    with pytest.raises(
        ValueError,
        match="missing a main process",
    ):
        shell.do_validate_task("BuildRoutes")
