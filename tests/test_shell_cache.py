# tests/test_shell_cache.py

from unittest.mock import Mock

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Cache listing
# ============================================================================


def test_list_cache_filters_hash_entries(
    shell,
    mock_cache,
):
    mock_cache.keys.return_value = [
        "task_a",
        "task_a_hashes",
        "task_b",
    ]

    result = shell._list_cache("")

    assert result == [
        "- 'task_a'",
        "- 'task_b'",
        "[bright_black]Total: 2[/]",
    ]


def test_list_cache_all_shows_hash_entries(
    shell,
    mock_cache,
):
    mock_cache.keys.return_value = [
        "task_a",
        "task_a_hashes",
    ]

    result = shell._list_cache("--all")

    assert result == [
        "- 'task_a'",
        "- 'task_a_hashes'",
        "[bright_black]Total: 2[/]",
    ]


# ============================================================================
# Public listing commands
# ============================================================================


def test_do_list_cache_prints_results(shell):
    shell._list_cache = Mock(
        return_value=[
            "one",
            "two",
        ]
    )

    shell.do_list_cache()

    shell._list_cache.assert_called_once_with("")
    shell.console.print.assert_any_call("one")
    shell.console.print.assert_any_call("two")


def test_cache_alias_calls_list_cache(shell):
    shell.do_list_cache = Mock()

    shell.do_cache("--all")

    shell.do_list_cache.assert_called_once_with("--all")


# ============================================================================
# Cache clearing
# ============================================================================


def test_clear_cache_requires_target(shell):
    shell.do_clear_cache("")

    shell.logger.shell_fail.assert_called_once_with("Task name required or use '.'")


def test_clear_cache_all(
    shell,
    mock_cache,
):
    mock_cache.keys.return_value = [
        "a",
        "b",
    ]

    shell.do_clear_cache(".")

    mock_cache.clear.assert_called_once_with()


def test_clear_cache_all_logs_progress(
    shell,
    mock_cache,
):
    mock_cache.keys.return_value = [
        "a",
        "b",
    ]

    shell.do_clear_cache(".")

    shell.logger.shell_info.assert_any_call("Clearing all cached data...")
    shell.logger.shell_info.assert_any_call("Cleared 2 cached result(s).")


def test_clear_cache_specific_task(
    shell,
    mock_cache,
):
    shell.do_clear_cache("task_a")

    mock_cache.clear.assert_called_once_with("task_a")


def test_clear_cache_specific_task_logs_progress(
    shell,
    mock_cache,
):
    shell.do_clear_cache("task_a")

    mock_cache.clear.assert_called_once_with("task_a")

    shell.logger.shell_info.assert_any_call("Clearing cache for task_a...")
    shell.logger.shell_info.assert_any_call("Complete.")


def test_clear_alias_calls_clear_cache(shell):
    shell.do_clear_cache = Mock()

    shell.do_clear("task_a")

    shell.do_clear_cache.assert_called_once_with("task_a")
