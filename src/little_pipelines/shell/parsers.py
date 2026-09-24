"""
Shell input parsers.

Responsibilities
----------------
Convert shell input strings into structured values.

This module should NOT:
    - Execute commands
    - Access Pipeline objects
    - Access Cache objects
    - Print to the console

It should only answer:

    "What did the user type?"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ============================================================================
# Execution
# ============================================================================

@dataclass(slots=True)
class ExecuteArgs:
    """
    Parsed arguments for the execute command.
    """

    target: str

    force: bool = False

    upstream: bool = True

    downstream: bool = True

    quiet: bool = False

    verbose: bool = False

    skip_tasks: list[str] | None = None

    kwargs: dict[str, Any] | None = None


# ============================================================================
# Low-level helpers
# ============================================================================


# def _get_skipped(inputs: list[str]) -> list[str]:
#     """
#     Extract skipped task names from shell arguments.

#     Example:
#         --skip=A --skip=B
#     """

#     return [
#         item.removeprefix("--skip=")
#         for item in inputs
#         if item.startswith("--skip=")
#     ]

# def _clean_kwargs(inputs: list[str]) -> dict[str, Any]:
#     """
#     Extract command kwargs.

#     Example:

#         execute MyTask --year=2025 --month=10

#     Returns:

#         {
#             "year": "2025",
#             "month": "10",
#         }
#     """

#     kwargs: dict[str, Any] = {}

#     for item in inputs:

#         if not item.startswith("--"):
#             continue

#         if "=" not in item:
#             continue

#         key, value = (
#             item.removeprefix("--")
#             .split("=", 1)
#         )

#         kwargs[key] = value

#     return kwargs


def split_input(inp: str) -> list[str]:
    """
    Split shell input into tokens.

    Examples
    --------
        "task_a --force"
            -> ["task_a", "--force"]

        ""
            -> []
    """

    return inp.split()


def extract_kwargs(inputs: list[str]) -> dict[str, str]:
    """
    Extract --key=value pairs.

    Examples
    --------
        ["--year=2025", "--month=10"]

        ->
        {
            "year": "2025",
            "month": "10",
        }
    """

    kwargs: dict[str, str] = {}

    for item in inputs:

        if not item.startswith("--"):
            continue

        if "=" not in item:
            continue

        key, value = (
            item
            .removeprefix("--")
            .split("=", 1)
        )

        kwargs[key] = value

    return kwargs


def extract_skip_tasks(
    inputs: list[str],
) -> list[str]:
    """
    Extract repeated --skip=<task> flags.

    Examples
    --------
        --skip=A --skip=B

        ->
        ["A", "B"]
    """

    return [
        item.removeprefix("--skip=")
        for item in inputs
        if item.startswith("--skip=")
    ]


# ============================================================================
# Execute command
# ============================================================================

def parse_execute_args(
    inp: str,
) -> ExecuteArgs:
    """
    Parse execute command arguments.

    Examples
    --------
        execute .

        execute . --force

        execute task_a

        execute task_a --force --year=2025
    """

    inputs = split_input(inp)

    if not inputs:
        raise ValueError(
            "Task name or '.' required."
        )

    if "--ignore" in inputs:
        raise NameError(
            "No --ignore flag. "
            "Did you mean --skip=<task> ?"
        )

    kwargs = extract_kwargs(inputs)

    # Remove shell-control kwargs
    for key in (
        "skip",
        "force",
        "quiet",
        "verbose",
    ):
        kwargs.pop(key, None)

    return ExecuteArgs(
        target=inputs[0],
        force="--force" in inputs,
        upstream="--no-upstream" not in inputs,
        downstream="--no-downstream" not in inputs,
        quiet=(
            "--quiet" in inputs
            or "-q" in inputs
        ),
        verbose=(
            "--verbose" in inputs
            or "-v" in inputs
        ),
        skip_tasks=extract_skip_tasks(inputs),
        kwargs=kwargs,
    )


__all__ = [
    "ExecuteArgs",
    "extract_kwargs",
    "extract_skip_tasks",
    "parse_execute_args",
    "split_input",
]
