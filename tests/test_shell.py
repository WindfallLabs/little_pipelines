# tests/test_shell.py

from unittest.mock import MagicMock, Mock, call

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_pipeline():
    pipeline = Mock()
    pipeline.name = "Test Pipeline"
    pipeline.list_tasks.return_value = []
    return pipeline


@pytest.fixture
def mock_cache():
    cache = Mock()
    cache.keys.return_value = []
    return cache


@pytest.fixture
def shell(mock_pipeline, mock_cache):
    sh = Shell(mock_pipeline, mock_cache)
    sh.logger = Mock()

    sh.console = MagicMock()
    sh.console.status.return_value.__enter__.return_value = None
    sh.console.status.return_value.__exit__.return_value = None
    return sh


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


def test_postcmd_exit_does_not_log(shell):
    shell.postcmd(False, "exit")

    shell.logger.shell_complete.assert_not_called()
    shell.logger.stop.assert_not_called()


def test_postcmd_blank_line_does_not_log(shell):
    shell.postcmd(False, "")

    shell.logger.shell_complete.assert_not_called()
    shell.logger.stop.assert_not_called()


def test_postcmd_logs_completion(shell):
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
        ("clear-cache abc", "clear_cache abc"),
        ("help list-cache", "help list_cache"),
        ("execute task", "execute task"),
    ],
)
def test_precmd_normalizes_dashes(shell, raw, expected):
    assert shell.precmd(raw) == expected


# ============================================================================
# Static helpers
# ============================================================================

def test_get_skipped_extracts_multiple_names():
    result = Shell._get_skipped(
        [
            "--skip=one",
            "--skip=two",
            "--force",
        ]
    )

    assert result == ["one", "two"]


def test_clean_kwargs_extracts_kwargs():
    result = Shell._clean_kwargs(
        [
            "--foo=bar",
            "--year=2025",
            "--force",
            "task_name",
        ]
    )

    assert result == {
        "foo": "bar",
        "year": "2025",
    }


def test_clean_kwargs_ignores_non_kwargs():
    result = Shell._clean_kwargs(
        [
            "task",
            "--force",
            "--verbose",
        ]
    )

    assert result == {}


# ============================================================================
# Cache helpers
# ============================================================================

def test_list_cache_filters_hash_entries(shell, mock_cache):
    mock_cache.keys.return_value = [
        "task_a",
        "task_a_hashes",
        "task_b",
    ]

    result = shell._list_cache("")

    assert "- 'task_a'" in result
    assert "- 'task_b'" in result
    assert not any("hashes" in item for item in result[:-1])


def test_list_cache_all_shows_hash_entries(shell, mock_cache):
    mock_cache.keys.return_value = [
        "task_a",
        "task_a_hashes",
    ]

    result = shell._list_cache("--all")

    assert "- 'task_a'" in result
    assert "- 'task_a_hashes'" in result


# ============================================================================
# Tasks command
# ============================================================================

def test_tasks_reports_totals(shell, mock_pipeline):
    mock_pipeline.list_tasks.return_value = [
        ("task_a", 3),
        ("task_b", 2),
    ]

    shell.do_tasks("")

    mock_pipeline.list_tasks.assert_called_once_with(True)

    printed = str(shell.console.print.call_args_list)

    assert "Total Tasks" in printed
    assert "Total Results" in printed


def test_tasks_sorts_when_requested(shell, mock_pipeline):
    mock_pipeline.list_tasks.return_value = [
        ("z_task", 1),
        ("a_task", 2),
    ]

    shell.do_tasks("--sort")

    calls = shell.console.print.call_args_list

    assert "a_task" in str(calls[0])
    assert "z_task" in str(calls[1])


# ============================================================================
# Clear cache
# ============================================================================

def test_clear_cache_all(shell, mock_cache):
    mock_cache.keys.return_value = ["a", "b"]

    shell.do_clear_cache(".")

    mock_cache.clear.assert_called_once_with()


def test_clear_cache_specific_task(shell, mock_cache):
    shell.do_clear_cache("task_a")

    mock_cache.clear.assert_called_once_with("task_a")


# ============================================================================
# Execution
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


def test_execute_single_task_with_flags(shell, mock_pipeline):
    shell._execute(
        "task_a "
        "--force "
        "--no-upstream "
        "--no-downstream "
        "--year=2025"
    )

    mock_pipeline.execute_one.assert_called_once_with(
        "task_a",
        force=True,
        upstream=False,
        downstream=False,
        year="2025",
    )


def test_execute_rejects_ignore_flag(shell):
    with pytest.raises(NameError):
        shell._execute("task_a --ignore")


def test_execute_restores_shell_verbosity(shell, mock_pipeline):
    shell._message_verbosity = "normal"

    shell._execute("task_a --quiet")

    assert shell.logger.set_verbosity.call_args_list[-1] == call("normal")


# ============================================================================
# onecmd error handling
# ============================================================================

def test_onecmd_logs_exceptions(shell):
    class BoomShell(Shell):
        def do_boom(self, inp):
            raise RuntimeError("kaboom")

    boom = BoomShell(shell.pipeline, shell.cache)
    boom.logger = Mock()
    boom.console = Mock()

    boom.onecmd("boom")

    boom.logger.shell_error.assert_called_once()


# ============================================================================
# Startup / shutdown hooks
# ============================================================================

def test_preloop_uses_default_startup(shell):
    shell._default_startup = Mock()

    shell.preloop()

    shell._default_startup.assert_called_once_with()


def test_preloop_calls_custom_startup(shell):
    shell.startup = Mock()

    shell.preloop()

    shell.startup.assert_called_once()


def test_preloop_falls_back_when_startup_fails(shell):
    shell.startup = Mock(side_effect=RuntimeError("boom"))
    shell._default_startup = Mock()

    shell.preloop()

    shell._default_startup.assert_called_once()


def test_postloop_uses_default_shutdown(shell):
    shell._default_shutdown = Mock()

    shell.postloop()

    shell._default_shutdown.assert_called_once_with()


def test_postloop_calls_custom_shutdown(shell):
    shell.shutdown = Mock()

    shell.postloop()

    shell.shutdown.assert_called_once()


def test_postloop_falls_back_when_shutdown_fails(shell):
    shell.shutdown = Mock(side_effect=RuntimeError("boom"))
    shell._default_shutdown = Mock()

    shell.postloop()

    shell._default_shutdown.assert_called_once()
