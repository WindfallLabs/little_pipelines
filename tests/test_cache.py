"""
Focused tests for the Cache object.

Cache responsibilities:
    - Result persistence
    - Result retrieval
    - Wildcard lookups
    - Insert modes
    - Serializer registration
    - Cache clearing
    - PipelineRun persistence

These tests intentionally avoid exercising Task behavior.
"""

import datetime as dt

import pytest

from little_pipelines.caching import Result
from little_pipelines.caching.serialize import Serializer
from little_pipelines.pipeline_run import PipelineRun
from little_pipelines.exc import (
    ResultExistsError,
    ResultNotFoundError,
)


# ==============================================================================
# Result CRUD
# ==============================================================================


def test_put_and_get_result(cache):
    result = Result(
        name="Example",
        task_name="MyTask",
        data=123,
    )

    cache.put(result)

    retrieved = cache.get("Example")

    assert retrieved.name == "Example"
    assert retrieved.task_name == "MyTask"
    assert retrieved.data == 123


def test_get_missing_result_raises(cache):
    with pytest.raises(ResultNotFoundError):
        cache.get("DoesNotExist")


def test_get_raw_rows(cache):
    cache.put(
        Result(
            name="Answer",
            task_name="Task",
            data=42,
        )
    )

    rows = cache.get(
        "Answer",
        return_raw_rows=True,
    )

    assert isinstance(rows, list)
    assert rows[0]["name"] == "Answer"
    assert rows[0]["task"] == "Task"


# ==============================================================================
# Wildcards
# ==============================================================================


def test_get_supports_name_wildcards(cache):
    cache.put(
        Result(
            name="Alpha",
            task_name="Task",
            data=1,
        )
    )

    cache.put(
        Result(
            name="Beta",
            task_name="Task",
            data=2,
        )
    )

    result = cache.get("Al*")

    assert result.name == "Alpha"
    assert result.data == 1


def test_get_for_task_supports_wildcards(cache):
    cache.put(
        Result(
            name="One",
            task_name="TaskA",
            data=1,
        )
    )

    cache.put(
        Result(
            name="Two",
            task_name="TaskB",
            data=2,
        )
    )

    results = cache.get_for_task("Task*")

    assert len(results) == 2


# ==============================================================================
# Insert Modes
# ==============================================================================


def test_put_requires_result_object(cache):
    with pytest.raises(TypeError):
        cache.put(123)


def test_put_invalid_mode_raises(cache):
    result = Result(
        name="Example",
        task_name="Task",
        data=1,
    )

    with pytest.raises(ValueError):
        cache.put(
            result,
            mode="BANANA",
        )


def test_put_fail_mode_raises_for_duplicate(cache):
    result = Result(
        name="Example",
        task_name="Task",
        data=1,
    )

    cache.put(result)

    with pytest.raises(ResultExistsError):
        cache.put(
            result,
            mode="FAIL",
        )


def test_put_upsert_replaces_existing_result(cache):
    cache.put(
        Result(
            name="Value",
            task_name="Task",
            data=1,
        )
    )

    cache.put(
        Result(
            name="Value",
            task_name="Task",
            data=999,
        ),
        mode="UPSERT",
    )

    assert cache.get("Value").data == 999


# ==============================================================================
# Keys
# ==============================================================================


def test_keys_returns_sorted_names(cache):
    cache.put(Result("C", "Task", 1))
    cache.put(Result("A", "Task", 1))
    cache.put(Result("B", "Task", 1))

    assert cache.keys() == [
        "A",
        "B",
        "C",
    ]


# ==============================================================================
# Clear
# ==============================================================================


def test_clear_single_result(cache):
    cache.put(
        Result(
            name="Value",
            task_name="Task",
            data=1,
        )
    )

    assert cache.clear("Value") is True

    assert cache.keys() == []


def test_clear_by_task_name(cache):
    cache.put(
        Result(
            name="One",
            task_name="TaskA",
            data=1,
        )
    )

    cache.put(
        Result(
            name="Two",
            task_name="TaskA",
            data=2,
        )
    )

    assert cache.clear("TaskA") is True

    assert cache.keys() == []


def test_clear_with_wildcard(cache):
    cache.put(
        Result(
            name="Alpha",
            task_name="Task",
            data=1,
        )
    )

    cache.put(
        Result(
            name="Beta",
            task_name="Task",
            data=2,
        )
    )

    assert cache.clear("A*") is True

    assert cache.keys() == ["Beta"]


def test_clear_missing_name_returns_false(cache):
    assert cache.clear("DoesNotExist") is False


def test_clear_entire_cache(cache):
    cache.put(
        Result(
            name="Value",
            task_name="Task",
            data=1,
        )
    )

    assert cache.clear() is True

    assert cache.keys() == []


# ==============================================================================
# Serializers
# ==============================================================================


def test_custom_serializer(cache):

    @cache.serializer(complex)
    class ComplexSerializer(Serializer):
        def dumps(self, data):
            return str(data).encode()

        def loads(self, data):
            return complex(data.decode())

    cache.put(
        Result(
            name="ComplexResult",
            task_name="Task",
            data=complex(1, 2),
        )
    )

    result = cache.get("ComplexResult")

    assert result.data == complex(1, 2)


def test_get_serializer_returns_default_for_unknown_type(cache):
    serializer = cache.get_serializer("<class 'imaginary'>")

    assert serializer is not None


# ==============================================================================
# Pipeline Runs
# ==============================================================================


def test_put_and_get_last_run(cache):
    run = PipelineRun(
        pipeline_name="Pipeline",
        start_time=dt.datetime.now(),
    )

    run.stop()

    cache.put_run(run)

    last_run = cache.get_last_run()

    assert last_run is not None
    assert last_run.run_id == run.run_id
    assert last_run.pipeline_name == "Pipeline"


def test_get_last_run_returns_none_when_empty(cache):
    assert cache.get_last_run() is None


def test_get_runs_returns_newest_first(cache):
    older = PipelineRun(
        pipeline_name="Pipeline",
        start_time=dt.datetime(2025, 1, 1, 12, 0, 0),
    )

    newer = PipelineRun(
        pipeline_name="Pipeline",
        start_time=dt.datetime(2025, 1, 1, 13, 0, 0),
    )

    cache.put_run(older)
    cache.put_run(newer)

    runs = cache.get_runs()

    assert runs[0].run_id == newer.run_id
    assert runs[1].run_id == older.run_id


def test_get_runs_filters_pipeline_name(cache):
    run_a = PipelineRun(
        pipeline_name="A",
        start_time=dt.datetime.now(),
    )

    run_b = PipelineRun(
        pipeline_name="B",
        start_time=dt.datetime.now(),
    )

    cache.put_run(run_a)
    cache.put_run(run_b)

    runs = cache.get_runs("A")

    assert len(runs) == 1
    assert runs[0].pipeline_name == "A"


def test_clear_runs_for_pipeline(cache):
    cache.put_run(
        PipelineRun(
            pipeline_name="A",
            start_time=dt.datetime.now(),
        )
    )

    cache.put_run(
        PipelineRun(
            pipeline_name="B",
            start_time=dt.datetime.now(),
        )
    )

    cache.clear_runs("A")

    runs = cache.get_runs()

    assert len(runs) == 1
    assert runs[0].pipeline_name == "B"


def test_clear_all_runs(cache):
    cache.put_run(
        PipelineRun(
            pipeline_name="A",
            start_time=dt.datetime.now(),
        )
    )

    cache.put_run(
        PipelineRun(
            pipeline_name="B",
            start_time=dt.datetime.now(),
        )
    )

    cache.clear_runs()

    assert cache.get_runs() == []


# ==============================================================================
# Connection
# ==============================================================================


def test_close(cache):
    cache.close()

    with pytest.raises(Exception):
        cache.keys()
