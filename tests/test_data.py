# tests/test_data.py

from unittest.mock import Mock

import pytest

from little_pipelines.data import Data
from little_pipelines.caching.result import Result


# ============================================================================
# Test isolation
# ============================================================================

@pytest.fixture(autouse=True)
def data_registry():
    """
    Data uses a global class-level registry.

    Clear it before and after every test to prevent
    order-dependent failures.
    """
    Data._registry.clear()
    yield
    Data._registry.clear()


# ============================================================================
# Construction
# ============================================================================

def test_init_stores_attributes():
    data = Data(
        "Parcels",
        dtype=dict,
        doc="Parcel dataset",
        source="County GIS",
        owner="Planning",
        tags=["gis"],
    )

    assert data.name == "Parcels"
    assert data.dtype is dict
    assert data.doc == "Parcel dataset"
    assert data.source == "County GIS"
    assert data.owner == "Planning"
    assert data.tags == ["gis"]


def test_init_defaults_tags_to_empty_list():
    data = Data("Parcels")

    assert data.tags == []


def test_init_registers_instance():
    data = Data("Parcels")

    assert Data.lookup("Parcels") is data


def test_extra_kwargs_available_via_getattr():
    data = Data(
        "Parcels",
        schema="public",
        refresh="daily",
    )

    assert data.schema == "public"
    assert data.refresh == "daily"


# ============================================================================
# Getter registration
# ============================================================================

def test_getter_decorator_registers_function():
    data = Data("Parcels")

    @data.getter
    def get(dataset):
        return "value"

    assert data._getter is get


def test_get_calls_registered_getter():
    data = Data("Parcels")

    @data.getter
    def get(dataset):
        return "value"

    assert data.get() == "value"


def test_get_passes_args_and_kwargs():
    data = Data("Parcels")

    received = {}

    @data.getter
    def get(dataset, *args, **kwargs):
        received["args"] = args
        received["kwargs"] = kwargs
        return "ok"

    data.get(False, 2025, county="Test")

    assert received["args"] == (2025,)
    assert received["kwargs"] == {
        "county": "Test",
    }


def test_get_without_getter_raises_attribute_error():
    data = Data("Parcels")

    with pytest.raises(AttributeError):
        data.get()


# ============================================================================
# Validation
# ============================================================================

def test_validator_decorator_registers_function():
    data = Data("Parcels")

    @data.validator
    def validate(dataset, value):
        return value

    assert data._validator is validate


def test_validate_without_validator_returns_original_value():
    data = Data("Parcels")

    obj = object()

    assert data.validate(obj) is obj


def test_validate_uses_registered_validator():
    data = Data("Parcels")

    @data.validator
    def validate(dataset, value):
        return value.upper()

    assert data.validate("hello") == "HELLO"


def test_get_validate_true_invokes_validator():
    data = Data("Parcels")

    @data.getter
    def get(dataset):
        return "hello"

    @data.validator
    def validate(dataset, value):
        return value.upper()

    assert data.get(validate=True) == "HELLO"


def test_get_validate_false_skips_validator():
    data = Data("Parcels")

    @data.getter
    def get(dataset):
        return "hello"

    validator = Mock(return_value="HELLO")
    data._validator = validator

    result = data.get()

    assert result == "hello"
    validator.assert_not_called()


# ============================================================================
# Result creation
# ============================================================================

def test_fulfill_returns_result():
    data = Data("Parcels")

    result = data.fulfill({"rows": 10})

    assert isinstance(result, Result)


def test_fulfill_uses_data_name_by_default():
    data = Data("Parcels")

    result = data.fulfill("value")

    assert result.name == "Parcels"
    assert result.data == "value"


def test_fulfill_accepts_custom_name():
    data = Data("Parcels")

    result = data.fulfill(
        "value",
        name="Custom Result",
    )

    assert result.name == "Custom Result"


def test_fulfill_passes_extra_metadata():
    data = Data("Parcels")

    result = data.fulfill(
        "value",
        extra={"source": "cache"},
    )

    assert result.extra == {"source": "cache"}


# ============================================================================
# Discovery
# ============================================================================

def test_lookup_returns_registered_object():
    original = Data("Parcels")

    found = Data.lookup("Parcels")

    assert found is original


def test_lookup_missing_name_raises_keyerror():
    with pytest.raises(KeyError):
        Data.lookup("DoesNotExist")


def test_all_returns_all_registered_objects():
    one = Data("One")
    two = Data("Two")

    result = Data.all()

    assert len(result) == 2
    assert one in result
    assert two in result


def test_all_empty_registry():
    assert Data.all() == []


def test_find_locals_returns_only_data_objects():
    one = Data("One")
    two = Data("Two")

    namespace = {
        "one": one,
        "two": two,
        "number": 123,
        "text": "hello",
        "object": object(),
    }

    result = Data.find_locals(namespace)

    assert set(result) == {"one", "two"}


# ============================================================================
# Status / policy integration
# ============================================================================

def test_status_without_policy_returns_unknown_status():
    data = Data("Parcels")

    result = data.status()

    assert result is not None


def test_status_delegates_to_policy():
    expected = object()

    policy = Mock()
    policy.check.return_value = expected

    data = Data(
        "Parcels",
        policy=policy,
    )

    result = data.status()

    assert result is expected
    policy.check.assert_called_once_with()


# ============================================================================
# Miscellaneous
# ============================================================================

def test_dependency_name_returns_name():
    data = Data("Parcels")

    assert data.dependency_name() == "Parcels"


def test_unknown_attribute_raises_attribute_error():
    data = Data("Parcels")

    with pytest.raises(AttributeError):
        data.not_real


def test_repr_with_dtype():
    data = Data(
        "Parcels",
        dtype=dict,
    )

    assert repr(data) == "<Data 'Parcels' (dict)>"


def test_repr_without_dtype():
    data = Data("Parcels")

    assert repr(data) == "<Data 'Parcels' (Any)>"
