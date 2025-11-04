"""Shared pytest fixtures and configuration."""

import atexit
from pathlib import Path

import pytest

import little_pipelines as lp


# ============================================================================
# CONFIG

PIPELINE_NAME = ".little-pipelines-tests"
REMOVE_TEST_DIRECTORY = False
DO_LOGGING = not REMOVE_TEST_DIRECTORY


# ============================================================================
# Helper Functions

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


# ============================================================================
# Fixtures

@pytest.fixture
def clean_pipeline():
    """Create a clean pipeline with automatic cleanup."""
    pipeline = lp.Pipeline(PIPELINE_NAME)
    pipeline.cache.clear()

    yield pipeline

    pipeline.cache.clear()
    if REMOVE_TEST_DIRECTORY:
        atexit.register(lambda: rm_dir(pipeline.cache))


@pytest.fixture
def sample_tasks():
    """Create sample tasks for testing (as in README example)."""
    zero = lp.Task(
        name="Zero",
        expire_results=lp.expire.never()
    )
    zero._enable_logging = DO_LOGGING

    @zero.process
    def run(this):
        return ["Some", "values"]

    one = lp.Task(
        name="One",
        dependencies=["Zero"],
        expire_results=lp.expire.never()
    )
    one._enable_logging = DO_LOGGING

    @one.process
    def preflight(this):
        return "OK"

    @one.process
    def run(this):
        status = this.preflight()
        data = this.pipeline.get_result("Zero")
        data.extend(["more", "values", status])
        return data

    return zero, one


@pytest.fixture
def pipeline_with_tasks(clean_pipeline, sample_tasks):
    """Pipeline pre-loaded with sample tasks."""
    pipeline = clean_pipeline
    zero, one = sample_tasks
    pipeline.add(zero, one)
    return pipeline, zero, one


@pytest.fixture
def executed_pipeline(pipeline_with_tasks):
    """Pipeline with tasks already executed."""
    pipeline, zero, one = pipeline_with_tasks
    pipeline.execute()
    return pipeline, zero, one
