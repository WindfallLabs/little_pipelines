import pytest

from little_pipelines import Cache, Task


@pytest.fixture
def cache():
    return Cache()


@pytest.fixture
def task(cache):
    task = Task(
        "Multi-Result Test",
        cache=cache
    )

    @task.main
    def main(t: Task):
        """Return two Results"""
        one = 1
        two = 2
        return (
            t.result(one, "One"),
            t.result(two, "Two")
        )
    
    return task


def test_multiple_results(task):
    results = task.main()
    assert results[0] == 1
    assert results[1] == 2


def test_unique_task_names(task):
    task.main()

    with pytest.raises(ValueError):
        task.result(1, "One")
