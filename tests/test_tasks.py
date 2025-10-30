import atexit
from pathlib import Path

import pytest

import little_pipelines as lp

PIPELINE_NAME = ".little-pipelines-tests"
REMOVE_TEST_DIRECTORY = True
DO_LOGGING = False


def rm_dir(cache):
    """Cleans up cache directory."""
    cache.close()

    try:
        (Path(cache.directory) / "cache.db").unlink()
    except:
        pass

    try:
        Path(cache.directory).rmdir()
    except:
        pass
    return


@pytest.fixture
def create_pipeline():
    """Exactly as in the README example."""
    zero = lp.Task(
        name="Zero"
    )
    zero._enable_logging = DO_LOGGING

    @zero.process
    def run(this):  # 'run' is a required decorated function
        return ["Some", "values"]

    one = lp.Task(
        name="One",
        dependencies=["Zero"]
    )
    one._enable_logging = DO_LOGGING

    @one.process
    def preflight(this):  # Some optional process
        return "OK"

    @one.process
    def run(this):  # 'this' is a self-like reference to the Task instance
        status = this.preflight()
        
        # Get data from dependency / upstream task
        data: list[str] = this.pipeline.get_result("Zero")
        # Manipulate it
        data.extend(["more", "values", status])
        return data  # ["Some", "values", "more", "values", "OK"]

    pipeline = lp.Pipeline(PIPELINE_NAME)
    pipeline.add(one, zero)  # Order doesn't matter

    yield pipeline, zero, one

    pipeline.cache.clear()
    if REMOVE_TEST_DIRECTORY:
        atexit.register(lambda: rm_dir(pipeline.cache))
    return


def test_execution_order(create_pipeline):
    """Test basic execution order."""
    pipeline, zero, one = create_pipeline
    tasks = [task for task in pipeline.tasks]
    assert pipeline.ntasks == 2
    assert tasks == [zero, one]
    assert not zero.is_executed
    assert not one.is_executed
    assert one.dependencies == {zero.name: zero}


def test_pipeline_execution(create_pipeline):
    """Test basic pipeline execution."""
    pipeline, zero, one = create_pipeline
    assert zero.result == None
    pipeline.execute()

    assert zero.is_executed == True
    assert zero.result == ["Some", "values"]
    assert one.is_executed == True
    assert one.result == ["Some", "values", "more", "values", "OK"]
    assert pipeline.is_complete == True


def test_checkpoints(create_pipeline):
    """Test checkpoint functionality."""
    # First execution
    pipeline, zero, one = create_pipeline
    pipeline.execute(force=True)  # Clear cache

    assert zero.is_executed == True, "Zero not executed"
    assert one.is_executed == True, "One not executed"

    # Second execution with fresh tasks (simulates rerunning script)
    pipeline2, zero2, one2 = create_pipeline
    pipeline2.execute()

    # Fresh tasks should use checkpoints (never actually run)
    assert zero2.is_executed == True  # Marked executed via checkpoint
    assert one2.is_executed == True


def test_force_tasks(create_pipeline):
    """Test forcing specific tasks to re-run."""
    # Initial run
    pipeline, zero, one = create_pipeline
    pipeline.execute()

    # Second run with fresh tasks, force One
    pipeline2, zero2, one2 = create_pipeline
    pipeline2.set_forced(one2.name)
    pipeline2.execute()

    # Both marked as executed (Zero from cache, One forced)
    assert zero2.is_executed == True
    assert one2.is_executed == True
    assert pipeline2.forced_tasks == ["One"]
    pipeline2.clear_forced()
    assert pipeline2.forced_tasks == []


def test_ignore_tasks(create_pipeline):
    """Test ignoring specific tasks."""
    pipeline, zero, one = create_pipeline

    # Execute with One ignored
    pipeline.set_ignored("One")
    assert len(pipeline.ignored_tasks) == 1
    pipeline.execute()

    # Zero should run, One should be skipped
    assert zero.is_executed == True
    assert one.is_executed == False
    assert one.is_skipped == True
    pipeline.clear_ignored()
    assert len(pipeline.ignored_tasks) == 0
