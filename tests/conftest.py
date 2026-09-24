"""
Shared pytest fixtures.

Only fixtures that are useful across multiple test modules
should live here.
"""

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
