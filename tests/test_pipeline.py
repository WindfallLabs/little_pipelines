"""
Tests for the Pipeline object.

Covers:
- Basic setup and repr
- Task registration and listing
- Topological execution order
- Skipping tasks (skip_tasks)
- Failure handling and downstream skipping
- get_upstream_tasks / get_downstream_tasks
- execute_one (target only, with upstream, with downstream)
- manual_execution_only tasks
"""

import pytest

import little_pipelines as lp
from little_pipelines import Cache, Task, Pipeline


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def cache():
    return Cache()


@pytest.fixture
def task_a(cache):
    """Root task -- no dependencies."""
    task = Task("A", cache=cache)

    @task.main
    def main(t: Task):
        return "a"

    return task


@pytest.fixture
def task_b(cache, task_a):
    """Depends on A."""
    task = Task("B", cache=cache, dependencies=["A"])

    @task.main
    def main(t: Task):
        upstream = t.dependencies["A"].data
        return upstream + "b"

    return task


@pytest.fixture
def task_c(cache, task_b):
    """Depends on B."""
    task = Task("C", cache=cache, dependencies=["B"])

    @task.main
    def main(t: Task):
        upstream = t.dependencies["B"].data
        return upstream + "c"

    return task


@pytest.fixture
def pipeline(task_a, task_b, task_c):
    """A simple three-task linear pipeline: A -> B -> C."""
    p = Pipeline("Test")
    p.add(task_a, task_b, task_c)
    return p


@pytest.fixture
def failing_task(cache):
    """A task whose main always raises."""
    task = Task("Failing", cache=cache)

    @task.main
    def main(t: Task):
        raise RuntimeError("boom")

    return task


# ==============================================================================
# Setup and repr
# ==============================================================================

def test_pipeline_repr(pipeline):
    assert "Test" in repr(pipeline)
    assert "3" in repr(pipeline)


def test_ntasks(pipeline):
    assert pipeline.ntasks == 3


def test_get_task_by_name(pipeline):
    task = pipeline.get_task("A")
    assert task.name == "A"


def test_get_task_missing_raises(pipeline):
    with pytest.raises(KeyError, match="No such task"):
        pipeline.get_task("Z")


# ==============================================================================
# Task listing
# ==============================================================================

def test_list_tasks(pipeline):
    names = pipeline.list_tasks()
    assert names == ["A", "B", "C"]


def test_list_tasks_with_cache_data(pipeline):
    pipeline.execute()
    details = pipeline.list_tasks(show_has_cached_data=True)
    # Each entry is (name, has_data, last_updated, reason)
    assert len(details) == 3
    names = [name for name, _, _, _ in details]
    assert names == ["A", "B", "C"]


def test_list_tasks_without_cache_data(pipeline):
    details = pipeline.list_tasks(show_has_cached_data=True)
    assert all(not has_data for _, has_data, _, _ in details)


# ==============================================================================
# Execution order and results
# ==============================================================================

def test_execute_runs_all_tasks(pipeline, cache):
    pipeline.execute()
    assert cache.get("A")[0].data == "a"
    assert cache.get("B")[0].data == "ab"
    assert cache.get("C")[0].data == "abc"


def test_execute_tasks_in_topological_order(pipeline):
    """Tasks must run in dependency order; C depends on B which depends on A."""
    execution_order = []

    for task in pipeline.tasks:
        original_main = task.main

        # Capture the name at definition time
        name = task.name
        def make_wrapper(n, m):
            def wrapper(*args, **kwargs):
                execution_order.append(n)
                return m(*args, **kwargs)
            return wrapper

        task.main = make_wrapper(name, original_main)

    pipeline.execute()
    assert execution_order == ["A", "B", "C"]


def test_is_complete_after_execute(pipeline):
    pipeline.execute()
    assert pipeline.is_complete is True


def test_is_not_complete_before_execute(pipeline):
    assert pipeline.is_complete is False


# ==============================================================================
# Skipping tasks
# ==============================================================================

def test_skip_tasks(pipeline, cache):
    pipeline.execute(skip_tasks=["B", "C"])
    assert cache.get("A")[0].data == "a"
    assert cache.get(task_name="B") == []
    assert cache.get(task_name="C") == []


def test_skipped_task_is_marked(pipeline, task_b):
    pipeline.execute(skip_tasks=["B", "C"])
    assert task_b.is_skipped is True


# ==============================================================================
# Failure handling
# ==============================================================================

def test_failed_task_returns_none(cache, failing_task):
    """execute() calls main(raise_errors=False), so exceptions are swallowed.
    A failing task produces no cached result and returns None."""
    p = Pipeline("FailTest")
    p.add(failing_task)
    p.execute()
    assert cache.get(task_name="Failing") == []


def test_downstream_errors_when_upstream_produces_nothing(cache):
    """If an upstream task fails silently (returns None, caches nothing),
    the downstream task will error when it tries to read the missing dependency."""
    producer = Task("Producer", cache=cache)

    @producer.main
    def main(t: Task):
        raise RuntimeError("upstream failure")

    consumer = Task("Consumer", cache=cache, dependencies=["Producer"])

    @consumer.main
    def main(t: Task):
        return t.dependencies["Producer"].data

    p = Pipeline("DownstreamTest")
    p.add(producer, consumer)
    p.execute()

    # Producer failed silently; Consumer errored trying to access the missing result
    assert cache.get(task_name="Producer") == []
    assert cache.get(task_name="Consumer") == []


# ==============================================================================
# get_upstream_tasks / get_downstream_tasks
# ==============================================================================

def test_get_upstream_tasks(pipeline):
    # B and A are both upstream of C
    upstream = pipeline.get_upstream_tasks("C")
    assert "A" in upstream
    assert "B" in upstream
    assert "C" not in upstream


def test_get_upstream_tasks_root_has_none(pipeline):
    assert pipeline.get_upstream_tasks("A") == []


def test_get_downstream_tasks(pipeline):
    # B and C are both downstream of A
    downstream = pipeline.get_downstream_tasks("A")
    assert "B" in downstream
    assert "C" in downstream
    assert "A" not in downstream


def test_get_downstream_tasks_leaf_has_none(pipeline):
    assert pipeline.get_downstream_tasks("C") == []


# ==============================================================================
# manual_execution_only
# ==============================================================================

def test_manual_task_not_executed_by_pipeline(cache):
    manual = Task("Manual", cache=cache, manual_execution_only=True)

    @manual.main
    def main(t: Task):
        return "manual result"

    p = Pipeline("ManualTest")
    p.add(manual)
    p.execute()

    assert manual.is_executed is False
    assert cache.get(task_name="Manual") == []


# ==============================================================================
# execute_one
# ==============================================================================

def test_execute_one_runs_target(pipeline, cache):
    pipeline.execute_one("B")
    assert cache.get("B")[0].data == "ab"


def test_execute_one_runs_upstream_by_default(pipeline, cache):
    """B depends on A; executing B should also run A."""
    pipeline.execute_one("B")
    assert cache.get("A")[0].data == "a"


def test_execute_one_runs_downstream_by_default(pipeline, cache):
    """Executing B should also run C, which depends on B."""
    pipeline.execute_one("B")
    assert cache.get("C")[0].data == "abc"


def test_execute_one_upstream_false(pipeline, cache):
    """With upstream=False, A should not be run automatically."""
    # Pre-populate A so B can read its dependency
    pipeline.get_task("A").main()
    pipeline.execute_one("B", upstream=False)
    # B ran, but we ran A manually -- just confirm B produced a result
    assert cache.get("B")[0].data == "ab"


def test_execute_one_downstream_false(pipeline, cache):
    """With downstream=False, C should not be run."""
    pipeline.execute_one("B", downstream=False)
    assert cache.get(task_name="C") == []
