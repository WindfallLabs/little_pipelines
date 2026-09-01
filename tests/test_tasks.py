"""
Tests for the Task object.

Covers:
- Basic task creation and execution
- Result caching (read from cache vs. re-execute)
- Dependencies between tasks
- Multiple results from a single task
- Error handling inside main()
- The `process` decorator
"""

import pytest

import little_pipelines as lp
from little_pipelines import Cache, Task, Result
from little_pipelines import exc


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def cache():
    return Cache()


@pytest.fixture
def simple_task(cache):
    """A basic task that returns a single integer."""
    task = Task("Simple", cache=cache)

    @task.main
    def main(t: Task):
        return 99

    return task


@pytest.fixture
def upstream_task(cache):
    """A task that another task will depend on."""
    task = Task("Upstream", cache=cache)

    @task.main
    def main(t: Task):
        return 10

    return task


@pytest.fixture
def downstream_task(cache, upstream_task):
    """A task that depends on the result of upstream_task."""
    task = Task("Downstream", cache=cache, dependencies=["Upstream"])

    @task.main
    def main(t: Task):
        upstream_result = t.dependencies["Upstream"].data
        return upstream_result * 2

    return task


@pytest.fixture
def multi_result_task(cache):
    """A task that returns two named results."""
    task = Task("Multi", cache=cache)

    @task.main
    def main(t: Task):
        return (
            t.result(1, "One"),
            t.result(2, "Two"),
        )

    return task


@pytest.fixture
def failing_task(cache):
    """A task whose main function raises an exception."""
    task = Task("Failing", cache=cache)

    @task.main
    def main(t: Task):
        raise ValueError("Something went wrong")

    return task


# ==============================================================================
# Basic execution
# ==============================================================================

def test_task_repr(simple_task):
    assert "Simple" in repr(simple_task)


def test_task_returns_value(simple_task):
    result = simple_task.main()
    assert result == 99


def test_task_result_stored_in_cache(simple_task, cache):
    simple_task.main()
    assert cache.get("Simple")[0].data == 99


def test_task_is_executed_after_main(simple_task):
    assert simple_task.is_executed is False
    simple_task.main()
    assert simple_task.is_executed is True


def test_task_not_executed_before_main(simple_task):
    assert simple_task.is_executed is False


# ==============================================================================
# Result caching: force=False reads from cache
# ==============================================================================

def test_cached_result_returned_on_second_call(cache):
    """With force=False, a second call should return the cached value."""
    call_count = 0

    task = Task("Counted", cache=cache)

    @task.main
    def main(t: Task):
        nonlocal call_count
        call_count += 1
        return call_count  # First call returns 1, second 2

    first_call = task.main(force=True)   # Run and cache (1)
    assert first_call == 1
    assert cache.keys() == ["Counted"]

    second_call = task.main(force=False)  # Should read from cache
    assert call_count == 1  # main() body ran only once
    assert second_call == 1  # Cached value returned


def test_force_reruns_task(cache):
    """With force=True (default), main() always re-executes."""
    call_count = 0

    task = Task("Rerun", cache=cache)

    @task.main
    def main(t: Task):
        nonlocal call_count
        call_count += 1
        return call_count

    first = task.main()
    second = task.main()

    assert first == 1
    assert second == 2
    assert call_count == 2


# ==============================================================================
# Dependencies
# ==============================================================================

def test_dependency_result_accessible(cache, upstream_task, downstream_task):
    upstream_task.main()
    result = downstream_task.main()
    assert result == 20


def test_missing_dependency_raises(cache):
    """Accessing a dependency that was never cached should raise."""
    task = Task("NoDeps", cache=cache, dependencies=["Ghost"])

    with pytest.raises(exc.DependencyNotFoundError):
        _ = task.dependencies["Ghost"]


def test_dependency_key_not_in_list_raises(cache, upstream_task, downstream_task):
    """Accessing a key not declared as a dependency raises a clear KeyError."""
    upstream_task.main()

    with pytest.raises(KeyError, match="not in Task.dependencies list"):
        _ = downstream_task.dependencies["NotADep"]


# ==============================================================================
# Multiple results
# ==============================================================================

def test_multi_result_values(multi_result_task):
    one, two = multi_result_task.main()
    assert one == 1
    assert two == 2


def test_multi_result_stored_by_name(multi_result_task, cache):
    multi_result_task.main()
    assert cache.get(result_name="One")[0].data == 1
    assert cache.get(result_name="Two")[0].data == 2


def test_duplicate_result_name_raises(cache):
    """Returning two results with the same name in one execution should raise."""
    task = Task("DupeNames", cache=cache)

    @task.main
    def main(t: Task):
        return (
            t.result(1, "Same"),
            t.result(2, "Same"),
        )

    with pytest.raises(ValueError):
        task.main()


def test_repeat_execution_does_not_raise(multi_result_task):
    """Running a multi-result task twice should not error."""
    multi_result_task.main()
    multi_result_task.main()  # Should not raise


# ==============================================================================
# Error handling
# ==============================================================================

def test_failing_task_raises_by_default(failing_task):
    with pytest.raises(ValueError, match="Something went wrong"):
        failing_task.main()


def test_failing_task_suppressed_with_raise_errors_false(failing_task):
    """raise_errors=False lets the pipeline continue without raising."""
    result = failing_task.main(raise_errors=False)
    assert result is None


# ==============================================================================
# process decorator
# ==============================================================================

def test_process_decorator(cache):
    """A method decorated with @task.process should run and return a value."""
    task = Task("Processed", cache=cache)

    @task.process
    def compute(t: Task):
        return 7 * 6

    @task.main
    def main(t: Task):
        return t.compute()

    result = task.main()
    assert result == 42
