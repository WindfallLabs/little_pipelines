# tests/test_shell_execution.py

from unittest.mock import Mock, call

import pytest


def test_execute_pipeline(shell, mock_pipeline):
    shell._execute(". --skip=foo --force")
    mock_pipeline.execute.assert_called_once_with(
        force_all=True,
        skip_tasks=["foo"],
    )


# ============================================================================
# Logging behavior


def test_reset_execution_logging_restores_shell_setting(shell):
    shell._message_verbosity = "verbose"
    shell._reset_execution_logging()
    shell.logger.set_verbosity.assert_called_once_with("verbose")


# ============================================================================
# Public command wrappers

def test_do_run_calls_execute(shell, monkeypatch):
    mock_execute = Mock()
    monkeypatch.setattr(shell, "_execute", mock_execute)
    shell.do_run("task_a")
    mock_execute.assert_called_once_with("task_a")


def test_do_execute_is_alias_for_run(shell, monkeypatch):
    mock_run = Mock()
    monkeypatch.setattr(shell, "do_run", mock_run)
    shell.do_execute("task_a")
    mock_run.assert_called_once_with("task_a")


# ============================================================================
# Reload commands

def test_reload_entire_pipeline(shell, mock_pipeline):
    shell.do_reload("")
    mock_pipeline.reload.assert_called_once_with()


def test_reload_single_task(shell, mock_pipeline):
    shell.do_reload("task_a")
    mock_pipeline.reload_task.assert_called_once_with("task_a")


def test_reload_strips_whitespace(shell, mock_pipeline):
    shell.do_reload("  task_a  ")
    mock_pipeline.reload_task.assert_called_once_with("task_a")
