"""
Tests for Task execution behavior.

Focus:
    - Task lifecycle
    - main() wrapper behavior
    - execution flags
    - return value handling
    - error propagation
"""

import pytest

from little_pipelines import Data, Task
from little_pipelines.exc import DuplicateResultsError


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def simple_task(cache):
    task = Task(
        "Simple",
        cache=cache,
    )

    @task.main
    def main(t):
        return 99

    return task


# =============================================================================
# Basic state
# =============================================================================


def test_task_repr_contains_name(simple_task):
    assert "Simple" in repr(simple_task)


def test_task_not_executed_before_main(simple_task):
    assert simple_task.is_executed is False


def test_task_is_executed_after_main(simple_task):
    simple_task.main()

    assert simple_task.is_executed is True


def test_process_wrapper(cache):
    task = Task("Process", cache=cache)

    @task.process
    def thing(t: Task, v: int):
        return 122 + v

    @task.main
    def main(t: Task):
        value = t.thing(1)
        return Data("Process").fulfill(value)

    assert task.main() == 123


# =============================================================================
# Return value handling
# =============================================================================


def test_single_scalar_return_is_unpacked(cache):
    task = Task(
        "Scalar",
        cache=cache,
    )

    @task.main
    def main(t):
        return 123

    assert task.main() == 123


def test_single_result_return_is_unpacked(cache):
    task = Task(
        "SingleResult",
        cache=cache,
    )

    @task.main
    def main(t):
        return Data(t.name).fulfill(456)

    assert task.main() == 456


def test_multi_result_return_is_tuple(cache):
    task = Task(
        "Multiple",
        cache=cache,
    )

    @task.main
    def main(t):
        return (
            Data("One").fulfill(1),
            Data("Two").fulfill(2),
        )

    assert task.main() == (1, 2)


def test_sequence_must_contain_only_results(cache):
    task = Task(
        "Mixed",
        cache=cache,
    )

    @task.main
    def main(t):
        return (
            Data("One").fulfill(1),
            2,
        )

    with pytest.raises(TypeError):
        task.main()


def test_string_return_is_not_treated_as_sequence(cache):
    task = Task(
        "StringTask",
        cache=cache,
    )

    @task.main
    def main(t):
        return "hello"

    assert task.main() == "hello"


def test_duplicate_result_name_raises(cache):
    task = Task(
        "Duplicates",
        cache=cache,
    )

    @task.main
    def main(t):
        return (
            Data("Same").fulfill(1),
            Data("Same").fulfill(2),
        )

    with pytest.raises(
        DuplicateResultsError,
        match="Multiple Results have the same name",
    ):
        task.main()


# =============================================================================
# Error handling
# =============================================================================


def test_task_exception_is_raised_by_default(cache):
    task = Task(
        "Failure",
        cache=cache,
    )

    @task.main
    def main(t):
        raise ValueError("boom")

    with pytest.raises(
        ValueError,
        match="boom",
    ):
        task.main()


def test_task_exception_suppressed_when_requested(cache):
    task = Task(
        "Failure",
        cache=cache,
    )

    @task.main
    def main(t):
        raise ValueError("boom")

    result = task.main(
        raise_errors=False,
    )

    assert result is None


def test_failed_task_is_not_executed(cache):
    task = Task(
        "Failure",
        cache=cache,
    )

    @task.main
    def main(t):
        raise ValueError("boom")

    with pytest.raises(ValueError):
        task.main()

    assert task.is_executed is False


# =============================================================================
# Wrapper kwarg validation
# =============================================================================


def test_force_kwarg_must_be_bool(cache):
    task = Task(
        "Example",
        cache=cache,
    )

    @task.main
    def main(t):
        return 1

    with pytest.raises(AttributeError):
        task.main(force="yes")


def test_raise_errors_kwarg_must_be_bool(cache):
    task = Task(
        "Example",
        cache=cache,
    )

    @task.main
    def main(t):
        return 1

    with pytest.raises(AttributeError):
        task.main(raise_errors="no")


# =============================================================================
# Wrapper behavior
# =============================================================================


def test_user_kwargs_are_passed_to_main(cache):
    task = Task(
        "Kwargs",
        cache=cache,
    )

    @task.main
    def main(t, value):
        return value

    assert task.main(value=123) == 123


def test_internal_kwargs_are_not_passed_to_main(cache):
    task = Task(
        "InternalKwargs",
        cache=cache,
    )

    @task.main
    def main(t):
        return 1

    assert (
        task.main(
            force=True,
            raise_errors=True,
        )
        == 1
    )


def test_task_can_execute_multiple_times(cache):
    counter = 0

    task = Task(
        "Repeatable",
        cache=cache,
    )

    @task.main
    def main(t):
        nonlocal counter
        counter += 1
        return counter

    assert task.main() == 1
    assert task.main() == 2
    assert task.main() == 3
