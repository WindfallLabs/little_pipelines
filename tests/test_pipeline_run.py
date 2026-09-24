"""
Focused tests for PipelineRun.

PipelineRun responsibilities:
    - Execution metadata
    - Lifecycle tracking
    - Duration calculations
    - Serialization
    - Deserialization
    - String representations
"""

import datetime as dt

from little_pipelines.pipeline_run import PipelineRun


# ==============================================================================
# Fixtures
# ==============================================================================


def test_pipeline_run_defaults():
    start = dt.datetime.now()

    run = PipelineRun(
        pipeline_name="TestPipeline",
        start_time=start,
    )

    assert run.pipeline_name == "TestPipeline"
    assert run.start_time is start
    assert run.end_time is None

    assert run.tasks_total == 0
    assert run.tasks_executed == 0
    assert run.tasks_skipped == 0
    assert run.tasks_failed == 0

    assert run.extra == {}
    assert isinstance(run.run_id, str)
    assert len(run.run_id) > 0


# ==============================================================================
# Lifecycle
# ==============================================================================


def test_stop_marks_run_complete():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
    )

    assert run.is_completed is False

    run.stop()

    assert run.is_completed is True
    assert run.end_time is not None


# ==============================================================================
# Success State
# ==============================================================================


def test_is_succeeded_true_when_no_failures():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
        tasks_failed=0,
    )

    assert run.is_succeeded is True


def test_is_succeeded_false_when_failures_exist():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
        tasks_failed=1,
    )

    assert run.is_succeeded is False


# ==============================================================================
# Duration
# ==============================================================================


def test_duration_is_none_when_running():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
    )

    assert run.duration is None
    assert run.duration_seconds is None


def test_duration_and_seconds_when_complete():
    start = dt.datetime(2025, 1, 1, 12, 0, 0)
    end = dt.datetime(2025, 1, 1, 12, 0, 5)

    run = PipelineRun(
        pipeline_name="Test",
        start_time=start,
        end_time=end,
    )

    assert run.duration == dt.timedelta(seconds=5)
    assert run.duration_seconds == 5.0


# ==============================================================================
# Serialization
# ==============================================================================


def test_to_record():
    start = dt.datetime(2025, 1, 1, 12, 0, 0)
    end = dt.datetime(2025, 1, 1, 12, 1, 0)

    run = PipelineRun(
        pipeline_name="Test",
        start_time=start,
        end_time=end,
        tasks_executed=10,
        tasks_skipped=2,
        tasks_failed=1,
        extra={"a": 1},
    )

    record = run.to_record()

    assert record["run_id"] == run.run_id
    assert record["pipeline_name"] == "Test"

    assert record["start_time"] == start.isoformat()
    assert record["end_time"] == end.isoformat()

    assert record["tasks_executed"] == 10
    assert record["tasks_skipped"] == 2
    assert record["tasks_failed"] == 1


def test_to_record_with_no_end_time():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
    )

    record = run.to_record()

    assert record["end_time"] is None


def test_from_record_round_trip():
    original = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
        end_time=dt.datetime.now(),
        tasks_executed=3,
        tasks_skipped=1,
        tasks_failed=2,
        extra={
            "user": "Garin",
            "version": 1,
        },
    )

    record = original.to_record()
    reconstructed = PipelineRun.from_record(record)

    assert reconstructed.run_id == original.run_id
    assert reconstructed.pipeline_name == original.pipeline_name
    assert reconstructed.start_time == original.start_time
    assert reconstructed.end_time == original.end_time

    assert reconstructed.tasks_executed == 3
    assert reconstructed.tasks_skipped == 1
    assert reconstructed.tasks_failed == 2

    assert reconstructed.extra == original.extra


def test_from_record_handles_empty_extra():
    record = {
        "run_id": "abc123",
        "pipeline_name": "Test",
        "start_time": dt.datetime.now().isoformat(),
        "end_time": None,
        "tasks_executed": 0,
        "tasks_skipped": 0,
        "tasks_failed": 0,
        "extra": None,
    }

    run = PipelineRun.from_record(record)

    assert run.extra == {}


# ==============================================================================
# Representations
# ==============================================================================


def test_str_running():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
    )

    assert str(run) == "Test (running)"


def test_str_completed():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime(2025, 1, 1, 12, 0, 0),
        end_time=dt.datetime(2025, 1, 1, 12, 0, 5),
        tasks_executed=3,
        tasks_skipped=1,
        tasks_failed=0,
    )

    text = str(run)

    assert "Test" in text
    assert "3 executed" in text
    assert "1 skipped" in text
    assert "0 failed" in text


def test_repr_contains_name_and_run_id():
    run = PipelineRun(
        pipeline_name="Test",
        start_time=dt.datetime.now(),
        run_id="1234567890abcdef",
    )

    text = repr(run)

    assert "PipelineRun" in text
    assert "Test" in text
    assert "12345678" in text
