import pytest

import little_pipelines as lp


@pytest.fixture
def cache():
    return lp.Cache2()


@pytest.fixture
def meaning_task(cache):
    task = lp.Task("Meaning", cache=cache)

    @task.main
    def main(t: lp.Task):
        return 42

    return task


@pytest.fixture
def addition_task(cache, meaning_task):
    add_task = lp.Task("Addition", cache=cache, dependencies=["Meaning"])

    @add_task.main
    def main(t: lp.Task):
        meaning = t.dependencies["Meaning"]
        return meaning.data + 8

    return add_task


def test_single_task(cache, meaning_task):
    r1 = meaning_task.main()

    assert r1 == 42
    assert cache.get("Meaning")[0].data == 42


def test_two_tasks(cache, meaning_task, addition_task):
    r1 = meaning_task.main()
    r2 = addition_task.main()

    assert r1 == 42
    assert cache.get("Meaning")[0].data == 42
    assert r2 == 50
    assert cache.get("Addition")[0].data == 50


def test_pipeline(cache, meaning_task, addition_task):
    pipeline = lp.Pipeline("Test")
    # TODO: is there a world where we prefer a pipeline.cache attr?
    pipeline.add(meaning_task)
    pipeline.add(addition_task)
    pipeline.execute()  # Executes the tasks in topological order

    assert cache.get("Meaning")[0].data == 42
    assert cache.get("Addition")[0].data == 50
    assert meaning_task.results[0] == 42
    assert addition_task.results[0] == 50
