"""
lp.Data tests
"""

import pytest

import little_pipelines as lp

DEFAULT = "Data placeholder"
BRONZE = "Bronze data placeholder"
GOLD = "Gold data placeholder"



def test_unnamed_layer():

    my_data = lp.Data("MyData")

    @my_data.getter
    def get(d: lp.Data, *args, **kwargs):
        return "Data placeholder"

    assert my_data.get() == DEFAULT
    assert my_data.data == DEFAULT
    assert my_data["default"] == DEFAULT
    assert "default" in my_data.layers


def test_single_named_layer():
    # All we do here is change the wrapped func name
    raw_data = "Raw data placeholder"

    my_data = lp.Data("MyData")

    @my_data.getter("raw")
    def raw(d: lp.Data, *args, **kwargs):
        return "Raw data placeholder"

    assert my_data.get("raw") == raw_data
    assert my_data.raw == raw_data
    assert my_data["raw"] == raw_data
    assert "raw" in my_data.layers


def test_default_named_func_layer():
    # Wrapped function's name shouldn't matter

    my_data = lp.Data("MyData")

    @my_data.getter
    def some_func_name_that_will_get_ignored(d: lp.Data, *args, **kwargs):
        return "Data placeholder"

    assert my_data.get() == DEFAULT
    assert my_data.data == DEFAULT
    assert my_data["default"] == DEFAULT
    assert "default" in my_data.layers


def test_multiple_layers():

    my_data = lp.Data("MyData")

    @my_data.getter
    def get(d: lp.Data, *args, **kwargs):
        return "Data placeholder"

    # Or with multiple layers
    @my_data.getter("bronze")
    def bronze(d: lp.Data, *args, **kwargs):
        return BRONZE

    @my_data.getter("gold")
    def gold(d: lp.Data, *args, **kwargs):
        return GOLD


    assert my_data.get() == DEFAULT
    assert my_data.data == DEFAULT
    assert my_data["default"] == DEFAULT
    assert "default" in my_data.layers

    assert my_data.get("bronze") == my_data["bronze"] == my_data.bronze == BRONZE
    assert my_data.get("gold") == my_data["gold"] == my_data.gold == GOLD
    assert "bronze" in my_data.layers
    assert "gold" in my_data.layers


def test_dtype_primative():

    my_data = lp.Data("MyData")

    @my_data.getter
    def get(d: lp.Data, *args, **kwargs):
        return 42

    my_data.set_dtype(int)

    assert my_data.layers["default"].dtype is int
    assert isinstance(my_data.data, my_data.layers["default"].dtype)


def test_dtype_decorated_classes():

    my_data = lp.Data("MyData")

    @my_data.set_dtype  # Default
    class TestType():
        def __init__(self):
            self.value = 1

    @my_data.getter
    def get(d: lp.Data, *args, **kwargs):
        return TestType()

    @my_data.set_dtype("two")
    class TestType2():
        def __init__(self):
            self.value = 42

    @my_data.getter("two")
    def get(d: lp.Data, *args, **kwargs):
        return TestType2()

    assert my_data.data.value == 1
    assert my_data.layers["default"].dtype is TestType
    assert isinstance(my_data.data, my_data.layers["default"].dtype)

    assert my_data.two.value == 42
    assert my_data.layers["two"].dtype is TestType2
    assert isinstance(my_data.two, my_data.layers["two"].dtype)


def test_validator():

    my_data = lp.Data("MyData")

    @my_data.getter
    def get(d: lp.Data, *args, **kwargs):
        return "42"

    @my_data.validator
    def validate(value):
        v = int(value)
        if v != 42:  # or not isinstance(v, d.dtype):
            raise ValueError("Wrong value")
        return v

    result = my_data.get(validate=True)
    assert result == 42
    assert my_data.layers["default"].validator(result)


def test_kwargs():
    # Test setting kwargs as properties

    my_data = lp.Data(
        "MyData",
        my_first_kwarg=42
    )

    assert my_data.my_first_kwarg == 42


def test_make_result():

    cache = lp.Cache()
    NAME = "MyData"
    my_data = lp.Data(NAME, cache=cache)
    r: lp.Result = my_data.set_result(42)

    assert isinstance(r, lp.Result)
    assert r.name == NAME
    assert r.data == 42


def test_cache_not_set():
    # Test no cache

    my_data = lp.Data("MyData")

    with pytest.raises(AttributeError):
        my_data.get("cache")


def test_cache():
    # Test no cache

    cache = lp.Cache()
    NAME = "MyData"
    my_data = lp.Data(NAME, cache=cache)
    r: lp.Result = my_data.set_result(42)
    cache.put(lp.Result(data=42, name=NAME, task_name=NAME))

    assert my_data.get("cache").data == 42
