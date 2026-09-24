# tests/test_shell.py

from unittest.mock import Mock

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Exit commands
# ============================================================================


def test_exit_returns_true(shell):
    assert shell.do_exit() is True


def test_quit_alias_calls_exit(shell):
    shell.do_exit = Mock(return_value=True)

    result = shell.do_quit()

    shell.do_exit.assert_called_once_with("")
    assert result is True


def test_q_alias_calls_exit(shell):
    shell.do_exit = Mock(return_value=True)

    result = shell.do_q()

    shell.do_exit.assert_called_once_with("")
    assert result is True


# ============================================================================
# Cmd hooks
# ============================================================================


def test_emptyline_returns_empty_string(shell):
    assert shell.emptyline() == ""


@pytest.mark.parametrize("command", ["exit", "quit", "q"])
def test_postcmd_exit_commands_do_not_log(shell, command):
    shell.postcmd(False, command)

    shell.logger.shell_complete.assert_not_called()
    shell.logger.stop.assert_not_called()


def test_postcmd_blank_line_does_not_log(shell):
    shell.postcmd(False, "")

    shell.logger.shell_complete.assert_not_called()
    shell.logger.stop.assert_not_called()


def test_postcmd_logs_completion_for_normal_command(shell):
    shell.postcmd(False, "tasks")

    shell.logger.shell_complete.assert_called_once_with("Ready")
    shell.logger.stop.assert_called_once()


# ============================================================================
# Input normalization
# ============================================================================


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("list-cache", "list_cache"),
        ("clear-cache task_a", "clear_cache task_a"),
        ("help list-cache", "help list_cache"),
        ("execute task_a", "execute task_a"),
    ],
)
def test_precmd_normalizes_command_names(
    shell,
    raw,
    expected,
):
    assert shell.precmd(raw) == expected


# ============================================================================
# Error handling
# ============================================================================


def test_onecmd_logs_exceptions(mock_pipeline, mock_cache):
    class BoomShell(Shell):
        def do_boom(self, inp):
            raise RuntimeError("kaboom")

    shell = BoomShell(mock_pipeline, mock_cache)
    shell.logger = Mock()
    shell.console = Mock()

    shell.onecmd("boom")

    shell.logger.shell_error.assert_called_once()


# ============================================================================
# Startup hooks
# ============================================================================


def test_preloop_uses_default_startup(shell):
    shell._default_startup = Mock()

    shell.preloop()

    shell._default_startup.assert_called_once_with()


def test_preloop_calls_custom_startup(shell):
    shell.startup = Mock()

    shell.preloop()

    shell.startup.assert_called_once_with()


def test_preloop_falls_back_to_default_when_startup_fails(
    shell,
):
    error = RuntimeError("boom")

    shell.startup = Mock(side_effect=error)
    shell._default_startup = Mock()

    shell.preloop()

    shell._default_startup.assert_called_once_with(error)


# ============================================================================
# Shutdown hooks
# ============================================================================


def test_postloop_uses_default_shutdown(shell):
    shell._default_shutdown = Mock()

    shell.postloop()

    shell._default_shutdown.assert_called_once_with()


def test_postloop_calls_custom_shutdown(shell):
    shell.shutdown = Mock()

    shell.postloop()

    shell.shutdown.assert_called_once_with()


def test_postloop_falls_back_to_default_when_shutdown_fails(
    shell,
):
    error = RuntimeError("boom")

    shell.shutdown = Mock(side_effect=error)
    shell._default_shutdown = Mock()

    shell.postloop()

    shell._default_shutdown.assert_called_once_with(error)
