# tests/test_shell_execution.py

from unittest.mock import Mock, call

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Core execution
# ============================================================================


def test_execute_pipeline(shell, mock_pipeline):
    shell._execute(". --skip=foo --force")

    mock_pipeline.execute.assert_called_once_with(
        force_all=True,
        skip_tasks=["foo"],
    )


def test_execute_single_task(shell, mock_pipeline):
    shell._execute("task_a")

    mock_pipeline.execute_one.assert_called_once_with(
        "task_a",
        force=False,
        upstream=True,
        downstream=True,
    )


def test_execute_single_task_with_flags(
    shell,
    mock_pipeline,
):
    shell._execute("task_a --force --no-upstream --no-downstream --year=2025")

    mock_pipeline.execute_one.assert_called_once_with(
        "task_a",
        force=True,
        upstream=False,
        downstream=False,
        year="2025",
    )


# ============================================================================
# Logging behavior
# ============================================================================


def test_execute_restores_shell_verbosity(
    shell,
):
    shell._message_verbosity = "normal"

    shell._execute("task_a --quiet")

    assert shell.logger.set_verbosity.call_args_list[-1] == call("normal")


def test_reset_execution_logging_restores_shell_setting(
    shell,
):
    shell._message_verbosity = "verbose"

    shell._reset_execution_logging()

    shell.logger.set_verbosity.assert_called_once_with("verbose")


def test_execute_draws_separator_rule(shell):
    shell._execute("task_a")

    shell.console.rule.assert_called_once_with(style="yellow")


def test_execute_restores_logging_when_execution_fails(
    shell,
    mock_pipeline,
):
    mock_pipeline.execute_one.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        shell._execute("task_a")

    assert shell.logger.set_verbosity.call_args_list[-1] == call(
        shell._message_verbosity
    )

    shell.console.rule.assert_called_once_with(style="yellow")


# ============================================================================
# Public command wrappers
# ============================================================================


def test_do_execute_calls_execute(shell):
    shell._execute = Mock()

    shell.do_execute("task_a")

    shell._execute.assert_called_once_with("task_a")


def test_run_alias_calls_execute(shell):
    shell.do_execute = Mock()

    shell.do_run("task_a")

    shell.do_execute.assert_called_once_with("task_a")


# ============================================================================
# Reload commands
# ============================================================================


def test_reload_entire_pipeline(
    shell,
    mock_pipeline,
):
    shell.do_reload("")

    mock_pipeline.reload.assert_called_once_with()


def test_reload_single_task(
    shell,
    mock_pipeline,
):
    shell.do_reload("task_a")

    mock_pipeline.reload_task.assert_called_once_with("task_a")


def test_reload_strips_whitespace(
    shell,
    mock_pipeline,
):
    shell.do_reload("  task_a  ")

    mock_pipeline.reload_task.assert_called_once_with("task_a")
