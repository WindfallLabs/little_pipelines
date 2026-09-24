"""
Focused tests for the Pipeline object.

Pipeline responsibilities:
    - Task registration
    - Dependency ordering
    - Graph traversal
    - Validation
    - Orchestration
    - PipelineRun tracking

These tests intentionally avoid exercising Task, Result,
and Cache internals except where required to observe
Pipeline behavior.
"""

import pytest

from little_pipelines import Cache, Pipeline, Task


# ==============================================================================
# Fixtures
# ==============================================================================

@pytest.fixture
def cache():
    return Cache()


@pytest.fixture
def pipeline(cache):
    a = Task("A", cache=cache)

    @a.main
    def main_a(t):
        return "a"

    b = Task(
        "B",
        cache=cache,
        dependencies=["A"],
    )

    @b.main
    def main_b(t):
        return "b"

    c = Task(
        "C",
        cache=cache,
        dependencies=["B"],
    )

    @c.main
    def main_c(t):
        return "c"

    p = Pipeline(
        "Test",
        cache=cache,
    )
    p.add(a, b, c)

    return p


# ==============================================================================
# Basics
# ==============================================================================

def test_pipeline_basics(pipeline):
    assert pipeline.ntasks == 3
    assert pipeline.get_task("A").name == "A"
    assert pipeline.list_tasks() == ["A", "B", "C"]


# ==============================================================================
# Graph
# ==============================================================================

def test_tasks_are_topologically_sorted(pipeline):
    assert [t.name for t in pipeline.tasks] == [
        "A",
        "B",
        "C",
    ]


def test_upstream_and_downstream_helpers(pipeline):
    assert pipeline.get_upstream_tasks("C") == [
        "A",
        "B",
    ]

    downstream = pipeline.get_downstream_tasks("A")

    assert "B" in downstream
    assert "C" in downstream


# ==============================================================================
# Validation
# ==============================================================================

def test_validate_missing_main(cache):
    task = Task(
        "Broken",
        cache=cache,
    )

    pipeline = Pipeline("Test")
    pipeline.add(task)

    with pytest.raises(ExceptionGroup):
        pipeline.validate_tasks()


def test_validate_duplicate_names(cache):
    a = Task(
        "Duplicate",
        cache=cache,
    )
    b = Task(
        "Duplicate",
        cache=cache,
    )

    @a.main
    def main_a(t):
        return 1

    @b.main
    def main_b(t):
        return 2

    pipeline = Pipeline("Test")
    pipeline.add(a, b)

    with pytest.raises(ExceptionGroup):
        pipeline.validate_tasks()


def test_validate_missing_dependency(cache):
    task = Task(
        "Consumer",
        cache=cache,
        dependencies=["MissingTask"],
    )

    @task.main
    def main(t):
        return None

    pipeline = Pipeline("Test")
    pipeline.add(task)

    with pytest.raises(ExceptionGroup):
        pipeline.validate_tasks()


def test_validate_cycle_detection(cache):
    a = Task(
        "A",
        cache=cache,
        dependencies=["B"],
    )

    b = Task(
        "B",
        cache=cache,
        dependencies=["A"],
    )

    @a.main
    def main_a(t):
        return "a"

    @b.main
    def main_b(t):
        return "b"

    pipeline = Pipeline("Test")
    pipeline.add(a, b)

    with pytest.raises(ExceptionGroup):
        pipeline.validate_tasks()


def test_validate_result_dependency(cache):
    producer = Task(
        "Producer",
        cache=cache,
        outputs={"Data": list},
    )

    consumer = Task(
        "Consumer",
        cache=cache,
        dependencies=["Data"],
    )

    @producer.main
    def main_a(t):
        return t.result([], name="Data")

    @consumer.main
    def main_b(t):
        return []

    p = Pipeline("Test")
    p.add(producer, consumer)

    p.validate_tasks()  # should not raise


def test_get_task_by_output_name(cache):
    t = Task(
        "Producer",
        cache=cache,
        outputs={"Parcels": list},
    )

    @t.main
    def main(task):
        return task.result([], name="Parcels")

    p = Pipeline("Test")
    p.add(t)

    assert p.get_task("Parcels") is t


# ==============================================================================
# Execution
# ==============================================================================

def test_execute_marks_pipeline_complete(pipeline):
    pipeline.execute()

    assert pipeline.is_complete is True


def test_execute_skip_tasks(pipeline):
    pipeline.execute(
        skip_tasks=["B"],
    )

    assert pipeline.get_task("B").is_skipped is True


def test_execute_one_runs_related_tasks(pipeline):
    pipeline.execute_one("B")

    assert pipeline.get_task("A").is_executed
    assert pipeline.get_task("B").is_executed
    assert pipeline.get_task("C").is_executed


def test_list_tasks_with_cache_counts(pipeline):
    pipeline.execute()

    tasks = pipeline.list_tasks(show_has_cached_data=True)

    assert tasks == [
        ("A", 1),
        ("B", 1),
        ("C", 1),
    ]


def test_list_tasks_handles_missing_results(cache):
    task = Task("A", cache=cache)

    @task.main
    def main(t):
        return 1

    p = Pipeline("Test")
    p.add(task)

    assert p.list_tasks(show_has_cached_data=True) == [
        ("A", 0),
    ]


# ==============================================================================
# PipelineRun
# ==============================================================================

def test_pipeline_run_tracking(pipeline):
    pipeline.execute()

    first_run = pipeline.current_run

    assert first_run.tasks_total == 3
    assert first_run.tasks_executed == 3

    pipeline.execute()

    assert pipeline.previous_run is first_run
