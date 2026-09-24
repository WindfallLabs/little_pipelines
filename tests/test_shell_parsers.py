# tests/test_shell_parsers.py

import pytest

from little_pipelines.shell.parsers import (
    extract_kwargs,
    extract_skip_tasks,
    parse_execute_args,
    split_input,
)


# ============================================================================
# split_input
# ============================================================================


def test_split_input_empty_string():
    assert split_input("") == []


def test_split_input_splits_tokens():
    assert split_input("task_a --force --year=2025") == [
        "task_a",
        "--force",
        "--year=2025",
    ]


# ============================================================================
# extract_kwargs
# ============================================================================


def test_extract_kwargs():
    result = extract_kwargs(
        [
            "--foo=bar",
            "--year=2025",
            "--force",
            "task_a",
        ]
    )

    assert result == {
        "foo": "bar",
        "year": "2025",
    }


def test_extract_kwargs_ignores_non_assignments():
    result = extract_kwargs(
        [
            "--force",
            "--quiet",
            "task_a",
        ]
    )

    assert result == {}


def test_extract_kwargs_preserves_first_equals():
    result = extract_kwargs(
        [
            "--sql=select * from table",
        ]
    )

    assert result == {
        "sql": "select * from table",
    }


# ============================================================================
# extract_skip_tasks
# ============================================================================


def test_extract_skip_tasks():
    result = extract_skip_tasks(
        [
            "--skip=one",
            "--skip=two",
            "--force",
        ]
    )

    assert result == [
        "one",
        "two",
    ]


def test_extract_skip_tasks_returns_empty_list():
    assert extract_skip_tasks([]) == []


# ============================================================================
# parse_execute_args
# ============================================================================


def test_parse_execute_requires_target():
    with pytest.raises(
        ValueError,
        match="Task name or '.' required",
    ):
        parse_execute_args("")


def test_parse_execute_pipeline():
    args = parse_execute_args(". --force --skip=foo")

    assert args.target == "."
    assert args.force is True
    assert args.skip_tasks == ["foo"]


def test_parse_execute_task():
    args = parse_execute_args("task_a")

    assert args.target == "task_a"
    assert args.force is False
    assert args.upstream is True
    assert args.downstream is True
    assert args.quiet is False
    assert args.verbose is False


def test_parse_execute_task_flags():
    args = parse_execute_args(
        "task_a --force --no-upstream --no-downstream --year=2025"
    )

    assert args.target == "task_a"
    assert args.force is True
    assert args.upstream is False
    assert args.downstream is False
    assert args.kwargs == {
        "year": "2025",
    }


@pytest.mark.parametrize(
    "flag",
    [
        "--quiet",
        "-q",
    ],
)
def test_parse_execute_quiet_flags(flag):
    args = parse_execute_args(f"task_a {flag}")

    assert args.quiet is True
    assert args.verbose is False


@pytest.mark.parametrize(
    "flag",
    [
        "--verbose",
        "-v",
    ],
)
def test_parse_execute_verbose_flags(flag):
    args = parse_execute_args(f"task_a {flag}")

    assert args.verbose is True
    assert args.quiet is False


def test_parse_execute_extracts_multiple_skip_tasks():
    args = parse_execute_args("task_a --skip=first --skip=second")

    assert args.skip_tasks == [
        "first",
        "second",
    ]


def test_parse_execute_removes_control_kwargs():
    args = parse_execute_args("task_a --force --quiet --skip=ignored --year=2025")

    assert args.kwargs == {
        "year": "2025",
    }


def test_parse_execute_rejects_ignore_flag():
    with pytest.raises(
        NameError,
        match="Did you mean --skip",
    ):
        parse_execute_args("task_a --ignore")
