import pytest

import little_pipelines as lp


@pytest.fixture
def cache():
    return lp.Cache()


@pytest.fixture
def meaning_task(cache):
    task = lp.Task("Meaning", cache=cache)

    @task.main
    def main(t: lp.Task):
        return 42

    return task


@pytest.fixture
def addition_task(cache, meaning_task):
    task = lp.Task("Addition", cache=cache, dependencies=["Meaning"])

    @task.process
    def add(t: lp.Task):
        meaning = t.dependencies["Meaning"].data
        r = meaning + 8
        return r
    
    @task.main
    def main(t: lp.Task):
        r: int = t.add()
        return r

    return task


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
