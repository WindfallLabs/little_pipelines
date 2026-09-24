"""
Tests for Task cache integration.

Focus:
    - Writing Results to the Cache
    - Reading cached Results
    - force execution behavior
    - Task.get_results()
"""

from little_pipelines import Task


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
    def main(t):
        nonlocal call_count
        call_count += 1
        return call_count

    first = task.main(force=True)
    cached = task.main(force=False)

    assert first == 1

    # Task body should only run once.
    assert call_count == 1

    # Cached path returns Result objects.
    assert len(cached) == 1
    assert cached[0].data == 1


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

    assert result.data == 123
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
    assert results[0].data == 42


def test_get_results_named_returns_mapping(cache):
    task = Task(
        "Example",
        cache=cache,
    )

    @task.main
    def main(t):
        return t.result(
            42,
            "Answer",
        )

    task.main()

    results = task.get_results(
        named=True,
    )

    assert set(results.keys()) == {
        "Answer",
    }

    assert results["Answer"].data == 42


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
            t.result(
                1,
                "One",
            ),
            t.result(
                2,
                "Two",
            ),
        )

    task.main()

    results = task.get_results(
        named=True,
    )

    assert set(results.keys()) == {
        "One",
        "Two",
    }

    assert results["One"].data == 1
    assert results["Two"].data == 2
