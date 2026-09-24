"""
Shared pytest fixtures.

Only fixtures that are useful across multiple test modules
should live here.
"""

from unittest.mock import MagicMock, Mock, call, patch

import pytest
import little_pipelines as lp


# ============================================================================
# Global test configuration


@pytest.fixture(autouse=True)
def quiet_logger():
    logger = lp.messaging.get_logger()
    logger.quiet = True
    logger.enabled = False

    yield


# ============================================================================
# Core fixtures


@pytest.fixture
def cache():
    """
    Fresh in-memory Cache.

    Most unit tests should depend on this fixture rather than
    constructing Cache objects themselves.
    """
    cache = lp.Cache()

    yield cache

    cache.close()


@pytest.fixture
def pipeline(cache):
    """
    Minimal Pipeline fixture.

    Intended for pipeline-focused tests.
    """
    pipeline = lp.Pipeline(
        "test-pipeline",
        cache=cache,
    )

    return pipeline


# ============================================================================
# Mocks


@pytest.fixture
def mock_pipeline():
    pipeline = Mock()
    pipeline.name = "Test Pipeline"
    return pipeline


@pytest.fixture
def mock_cache():
    cache = Mock()
    cache.keys.return_value = []
    return cache


@pytest.fixture
def task_factory():
    def build(**kwargs):
        task = Mock()

        task.name = kwargs.get("name", "Task")
        task.has_main = kwargs.get("has_main", True)
        task.outputs = kwargs.get("outputs", {})
        task.dependency_names = kwargs.get(
            "dependency_names",
            set(),
        )

        return task

    return build


@pytest.fixture
def dataset_factory():
    def build(**kwargs):
        ds = Mock()
        ds.name = kwargs.get("name", "Parcels")
        ds.dtype = kwargs.get("dtype", str)
        ds.owner = kwargs.get("owner", None)
        ds.source = kwargs.get("source", None)
        ds.tags = kwargs.get("tags", [])
        ds.doc = kwargs.get("doc", None)
        return ds

    return build


# ============================================================================
# Shell


@pytest.fixture
def shell(mock_pipeline, mock_cache):
    shell = lp.Shell(mock_pipeline, mock_cache)

    shell.logger = Mock()
    shell.console = MagicMock()

    shell.console.status.return_value.__enter__.return_value = None
    shell.console.status.return_value.__exit__.return_value = None

    return shell
