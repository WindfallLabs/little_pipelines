"""
Tests for Task output contracts and validation.
"""


import pytest

from little_pipelines import DataSpec, Task
from little_pipelines.exc import (
    MissingOutputError,
    TaskOutputValidationError,
    UnexpectedOutputError,
)


def test_multiple_outputs_pass_validation(cache):

    a = DataSpec("a", dtype=int)
    b = DataSpec("b", dtype=str)

    task = Task(
        "Multiple",
        cache=cache,
        outputs=[a, b],
    )

    @task.main
    def main(t):
        return (
            a.fulfill(123),
            b.fulfill("abc")
        )

    raw_results = task.main()
    assert raw_results == (123, "abc")


def test_wrong_output_type_raises(cache):
    data = DataSpec("value", dtype=int)
    task = Task(
        "Typed",
        cache=cache,
        outputs=[data],
    )

    @task.main
    def main(t):
        return data.fulfill("abc")  # Error raised by resultify

    with pytest.raises(TaskOutputValidationError):
        task.main()


def test_missing_output_raises(cache):
    expected = DataSpec("expected", dtype=int)
    observed = DataSpec("observed", dtype=int)
    task = Task(
        "Missing",
        cache=cache,
        outputs=[expected],
    )

    @task.main
    def main(t):
        return observed.fulfill(123)  # not 'expected'

    with pytest.raises(MissingOutputError):
        task.main()


def test_no_outputs_returned_raises(cache):
    expected = DataSpec("expected", dtype=int)
    task = Task(
        "MissingAll",
        cache=cache,
        outputs=[expected],
    )

    @task.main
    def main(t):
        return ()  # Nothing

    with pytest.raises(MissingOutputError):
        task.main()


def test_unexpected_output_raises(cache):
    expected = DataSpec("expected")
    extra = DataSpec("extra")
    task = Task(
        "Unexpected",
        cache=cache,
        outputs=[expected],
    )

    @task.main
    def main(t):
        return (
            expected.fulfill(123),
            extra.fulfill(456),
        )

    with pytest.raises(UnexpectedOutputError):
        task.main()


def test_any_output_type_accepts_any_value(cache):
    data = DataSpec("value")
    task = Task(
        "AnyOutput",
        cache=cache,
        outputs=[data],
    )

    @task.main
    def main(t):
        return data.fulfill({"anything": ["goes", 123]})

    result = task.main()

    assert result == {"anything": ["goes", 123]}


def test_data_validation_errors_are_wrapped(cache):
    data = DataSpec(
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

    with pytest.raises(RuntimeError):
        task.main()
