"""
Tests for Task dependency management.

Fous:
    - dependency registration
    - dependency resolution
    - dependency access
    - dependency-related exceptions
"""

import pytest

from little_pipelines import Data, Task
from little_pipelines import exc


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def upstream_task(cache):
    task = Task(
        "Upstream",
        cache=cache,
    )

    @task.main
    def main(t):
        return 10

    return task


@pytest.fixture
def downstream_task(cache):
    task = Task(
        "Downstream",
        cache=cache,
        dependencies=["Upstream"],
    )

    @task.main
    def main(t):
        return t.dependencies["Upstream"].data * 2

    return task


# =============================================================================
# Dependency registration
# =============================================================================


def test_string_dependencies_are_normalized():
    task = Task(
        "Example",
        dependencies=[
            "A",
            "B",
        ],
    )

    assert task.dependency_names == frozenset({"A", "B"})


def test_data_dependencies_are_normalized():
    upstream = Data(
        "Parcels",
    )

    task = Task(
        "Example",
        dependencies=[upstream],
    )

    assert task.dependency_names == frozenset({"Parcels"})


def test_duplicate_dependencies_are_deduplicated():
    task = Task(
        "Example",
        dependencies=[
            "A",
            "A",
            "B",
        ],
    )

    assert task.dependency_names == frozenset({"A", "B"})


# =============================================================================
# Dependency resolution
# =============================================================================


def test_dependency_result_accessible(
    upstream_task,
    downstream_task,
):
    upstream_task.main()

    result = downstream_task.main()

    assert result == 20


def test_dependencies_returns_result_objects(
    upstream_task,
    downstream_task,
):
    upstream_task.main()

    deps = downstream_task.dependencies

    assert deps["Upstream"].data == 10
    assert deps["Upstream"].task_name == "Upstream"


# =============================================================================
# Dependency failures
# =============================================================================


def test_missing_dependency_raises(cache):
    task = Task(
        "NoDeps",
        cache=cache,
        dependencies=[
            "Ghost",
        ],
    )

    with pytest.raises(
        exc.DependencyNotFoundError,
    ):
        _ = task.dependencies


def test_dependency_key_not_in_list_raises(
    upstream_task,
    downstream_task,
):
    upstream_task.main()

    with pytest.raises(
        KeyError,
        match="not in Task.dependencies list",
    ):
        _ = downstream_task.dependencies["NotADep"]
