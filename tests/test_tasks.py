from pathlib import Path

import little_pipelines as lp

TEST_CACHE = "tests"


def create_pipeline(pipeline_name):
    zero = lp.Task(
        name="Zero",
        log_path="DISABLE"
    )

    @zero.process
    def run(this, *args, **kwargs):
        return ["Some", "values"]

    one = lp.Task(
        name="One",
        dependencies=["Zero"],
        log_path="DISABLE"
    )

    @one.process
    def preflight(this, *args, **kwargs):
        return "OK"

    @one.process
    def run(this, *args, **kwargs):
        status = this.preflight()
        
        # Get data from upstream task
        data: list[str] = this.pipeline.get_result("Zero")
        data.extend(["more", "values"])
        return data

    pipeline = lp.Pipeline(
        name=pipeline_name,
        cache_name=TEST_CACHE
    )
    pipeline.cache.clear()
    pipeline.add(
        zero,
        one,
    )
    return pipeline, zero, one


def test_execution_order():
    """Test basic execution order."""
    pipeline, zero, one = create_pipeline("test_pipeline")
    #pipeline._tasks = []
    #assert one.dependencies is None
    #pipeline.add(zero, one)
    tasks = [task for task in pipeline.tasks]
    assert pipeline.ntasks == 2
    assert tasks == [zero, one]
    assert not zero.is_executed
    assert not one.is_executed
    assert one.dependencies == {zero.name: zero}


def test_pipeline_execution():
    """Test basic pipeline execution."""
    pipeline, zero, one = create_pipeline("test_pipeline")
    assert zero.result == None
    pipeline.execute()

    assert zero.is_executed == True
    assert zero.result == ["Some", "values"]
    assert one.is_executed == True
    assert one.result == ["Some", "values", "more", "values"]
    assert pipeline.is_complete == True


def test_checkpoints():
    """Test checkpoint functionality."""
    # First execution
    pipeline, zero, one = create_pipeline("test_pipeline")
    pipeline.execute(force=True)  # Clear cache

    assert zero.is_executed == True, "Zero not executed"
    assert one.is_executed == True, "One not executed"

    # Second execution with fresh tasks (simulates rerunning script)
    pipeline2, zero2, one2 = create_pipeline("test_checkpoints")
    pipeline2.execute()

    # Fresh tasks should use checkpoints (never actually run)
    assert zero2.is_executed == True  # Marked executed via checkpoint
    assert one2.is_executed == True


def test_force_tasks():
    """Test forcing specific tasks to re-run."""
    # Initial run
    pipeline, zero, one = create_pipeline("test_pipeline")
    pipeline.execute()

    # Second run with fresh tasks, force One
    pipeline2, zero2, one2 = create_pipeline("test_pipeline")
    pipeline2.set_forced(one2.name)
    pipeline2.execute()

    # Both marked as executed (Zero from cache, One forced)
    assert zero2.is_executed == True
    assert one2.is_executed == True
    assert pipeline2.forced_tasks == ["One"]
    pipeline2.clear_forced()
    assert pipeline2.forced_tasks == []


def test_ignore_tasks():
    """Test ignoring specific tasks."""
    pipeline, zero, one = create_pipeline("test_ignore")

    # Execute with One ignored
    pipeline.set_ignored("One")
    assert len(pipeline.ignored_tasks) == 1
    pipeline.execute()

    # Zero should run, One should be skipped
    assert zero.is_executed == True
    assert one.is_executed == False
    assert one.skipped == True
    pipeline.clear_ignored()
    assert len(pipeline.ignored_tasks) == 0
