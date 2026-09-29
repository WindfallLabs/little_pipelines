"""
Tests for Task cache integration.

Focus:
    - Writing Results to the Cache
    - Reading cached Results
    - force execution behavior
    - Task.get_results()
"""

from little_pipelines import DataSpec, Task

# =============================================================================
# Cached execution
# =============================================================================


def test_force_false_uses_cached_results(cache):
    call_count = 0

    task = Task(
        "Counted",
        cache=cache,
    )

    @task.main
    def main(t) -> int:
        nonlocal call_count
        call_count += 1
        return call_count

    first: int = task.main(force=True)
    assert first == 1

    cached: int = task.main(force=False)
    assert cached == 1
    assert first == 1
    assert call_count == 1


def test_force_true_reruns_task(cache):
    call_count = 0

    task = Task(
        "Rerun",
        cache=cache,
    )

    @task.main
    def main(t):
        nonlocal call_count
        call_count += 1
        return call_count

    first = task.main(force=True)
    second = task.main(force=True)

    assert first == 1
    assert second == 2
    assert call_count == 2


# =============================================================================
# Cache writes
# =============================================================================


def test_task_writes_result_to_cache(cache):
    task = Task(
        "Writer",
        cache=cache,
    )

    @task.main
    def main(t):
        return 123

    task.main()

    result = cache.get("Writer")

    assert result.value == 123
    assert result.task_name == "Writer"


# =============================================================================
# get_results()
# =============================================================================


def test_get_results_returns_cached_results(cache):
    task = Task(
        "Example",
        cache=cache,
    )

    @task.main
    def main(t):
        return 42

    task.main()

    results = task.get_results()

    assert len(results) == 1
    assert results[0].value == 42


def test_get_results_named_returns_mapping(cache):
    task = Task(
        "Example",
        cache=cache,
    )

    @task.main
    def main(t):
        return DataSpec("Answer").fulfill(42)

    task.main()

    results = task.get_results(named=True)
    assert set(results.keys()) == {"Answer"}
    assert results["Answer"].value == 42


# =============================================================================
# Multiple cached Results
# =============================================================================


def test_get_results_returns_multiple_cached_results(cache):
    task = Task(
        "Multiple",
        cache=cache,
    )

    @task.main
    def main(t):
        return (
            DataSpec("One").fulfill(1),
            DataSpec("Two").fulfill(2)
        )

    task.main()

    results = task.get_results(named=True)

    assert set(results.keys()) == {"One", "Two"}
    assert results["One"].value == 1
    assert results["Two"].value == 2
