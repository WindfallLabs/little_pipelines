import pytest

from little_pipelines import Cache, Result, Task
from little_pipelines import exc


def test_not_cached():
    cache = Cache()
    
    task = Task("Not Cached", cache=cache, dependencies=["Doesn't Exist"])

    with pytest.raises(exc.DependencyNotFoundError):
        task.dependencies["Doesn't Exist"]


def test_result_caching():
    cache = Cache()
    result_name = "TEST-RESULT"
    task_name = "TEST-TASK"
    extra_data = {"NOTE": "Hello"}

    result = Result(result_name, task_name, 42, extra=extra_data)
    cache.put(result)

    # Test get-results
    assert cache.get(result_name=result_name)[0] == result
    assert cache.get("TEST-R%")[0] == result
    assert cache.get("TEST-R*")[0] == result

    assert cache.get(task_name=task_name)[0] == result
    assert cache.get(task_name="TEST-T*")[0] == result

    # Test raw-rows
    r = cache.get("TEST-*", return_raw_rows=True)
    assert r == [{
        "name": "TEST-RESULT",
        "task": "TEST-TASK",
        "dtype": "<class 'int'>",
        "last_updated": result.last_updated.strftime(result._datetime_format),
        "expiry": None,
        "data": b"\x80\x04K*.",
        "extra": '{"NOTE": "Hello"}'
    }]

    # Test clear
    assert len(cache.keys()) > 0
    assert cache.clear("TEST-*") is True
    assert cache.keys() == []

