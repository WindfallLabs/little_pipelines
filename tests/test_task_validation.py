"""
Tests for Task output contracts and validation.
"""

from typing import Any

import pytest

from little_pipelines import Data, Task
from little_pipelines.exc import (
    MissingOutputError,
    TaskOutputValidationError,
    UnexpectedOutputError,
)


# =============================================================================
# Output contract normalization
# =============================================================================

def test_outputs_property_reflects_dict_contract():
    task = Task(
        "Example",
        outputs={
            "value": int,
            "text": str,
        },
    )

    assert task.outputs == {
        "value": int,
        "text": str,
    }


def test_outputs_none_creates_no_contract():
    task = Task("Example")

    assert task.outputs == {}


def test_data_output_contract_exposes_dtype():
    data = Data(
        name="Output",
        dtype=int,
    )

    task = Task(
        "Example",
        outputs=[data],
    )

    assert task.outputs == {
        "Output": int,
    }


def test_invalid_output_name_raises():
    with pytest.raises(TypeError):
        Task(
            "Bad",
            outputs={
                123: int,
            },
        )


def test_invalid_output_type_raises():
    with pytest.raises(TypeError):
        Task(
            "Bad",
            outputs={
                "value": "int",
            },
        )


def test_invalid_outputs_container_raises():
    with pytest.raises(TypeError):
        Task(
            "Bad",
            outputs="not-valid",
        )


def test_outputs_list_must_contain_data_objects():
    with pytest.raises(TypeError):
        Task(
            "Bad",
            outputs=[
                "not-a-data-object",
            ],
        )


# =============================================================================
# Dict-based output contracts
# =============================================================================

def test_correct_output_type_passes(cache):
    task = Task(
        "Typed",
        cache=cache,
        outputs={
            "value": int,
        },
    )

    @task.main
    def main(t):
        return Data("value").fulfill(123)

    assert task.main() == 123


def test_multiple_outputs_pass_validation(cache):
    task = Task(
        "Multiple",
        cache=cache,
        outputs={
            "a": int,
            "b": str,
        },
    )

    @task.main
    def main(t):
        return (
            Data("a").fulfill(123),
            Data("b").fulfill("abc"),
        )

    assert task.main() == (123, "abc")


def test_wrong_output_type_raises(cache):
    task = Task(
        "Typed",
        cache=cache,
        outputs={
            "value": int,
        },
    )

    @task.main
    def main(t):
        return Data("value").fulfill("abc")

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    assert any(
        isinstance(e, TaskOutputValidationError)
        for e in exc_info.value.exceptions
    )


def test_missing_output_raises(cache):
    task = Task(
        "Missing",
        cache=cache,
        outputs={
            "expected": int,
        },
    )

    @task.main
    def main(t):
        return Data("other").fulfill(123)

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    assert any(
        isinstance(e, MissingOutputError)
        for e in exc_info.value.exceptions
    )


def test_no_outputs_returned_raises(cache):
    task = Task(
        "MissingAll",
        cache=cache,
        outputs={
            "a": int,
            "b": str,
        },
    )

    @task.main
    def main(t):
        return ()

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    missing = [
        e
        for e in exc_info.value.exceptions
        if isinstance(e, MissingOutputError)
    ]

    assert len(missing) == 2


def test_unexpected_output_raises(cache):
    task = Task(
        "Unexpected",
        cache=cache,
        outputs={
            "expected": int,
        },
    )

    @task.main
    def main(t):
        return (
            Data("expected").fulfill(123),
            Data("extra").fulfill(456),
        )

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    assert any(
        isinstance(e, UnexpectedOutputError)
        for e in exc_info.value.exceptions
    )


def test_any_output_type_accepts_any_value(cache):
    task = Task(
        "AnyOutput",
        cache=cache,
        outputs={
            "value": Any,
        },
    )

    @task.main
    def main(t):
        return Data("value").fulfill(
            {"anything": ["goes", 123]}
        )

    result = task.main()

    assert result == {"anything": ["goes", 123]}


def test_data_validation_errors_are_wrapped(cache):
    data = Data(
        name="Validated",
        dtype=int,
    )

    @data.validator
    def validate(d, value):
        raise RuntimeError("validator exploded")

    task = Task(
        "ValidatedTask",
        cache=cache,
        outputs=[data],
    )

    @task.main
    def main(t):
        return data.fulfill(123)

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    assert any(
        isinstance(
            exc,
            TaskOutputValidationError,
        )
        for exc in exc_info.value.exceptions
    )


# =============================================================================
# Error aggregation
# =============================================================================

def test_multiple_validation_errors_are_grouped(cache):
    task = Task(
        "Grouped",
        cache=cache,
        outputs={
            "a": int,
            "b": str,
        },
    )

    @task.main
    def main(t):
        return (
            Data("a").fulfill("wrong type"),
            Data("unexpected").fulfill(123),
        )

    with pytest.raises(ExceptionGroup) as exc_info:
        task.main()

    assert len(exc_info.value.exceptions) >= 3
