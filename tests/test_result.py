# tests/test_result.py

import datetime as dt

from little_pipelines.caching.result import Result


# ============================================================================
# Construction
# ============================================================================

def test_init_stores_attributes():
    now = dt.datetime(2025, 1, 1)

    result = Result(
        name="Parcels",
        data={"a": 1},
        task_name="LoadParcels",
        dtype="dict",
        last_updated=now,
        expiry=None,
        extra={"source": "test"},
    )

    assert result.name == "Parcels"
    assert result.data == {"a": 1}
    assert result.task_name == "LoadParcels"
    assert result.dtype == "dict"
    assert result.last_updated == now
    assert result.expiry is None
    assert result.extra == {"source": "test"}


def test_dtype_defaults_from_data():
    result = Result(
        name="Parcels",
        data={"a": 1},
        task_name="LoadParcels",
    )

    assert result.dtype == str(type({"a": 1}))


def test_last_updated_defaults_to_now():
    before = dt.datetime.now()

    result = Result(
        name="Parcels",
        data={},
        task_name="LoadParcels",
    )

    after = dt.datetime.now()

    assert before <= result.last_updated <= after


def test_expiry_defaults_to_none():
    result = Result(
        name="Parcels",
        data={},
        task_name="LoadParcels",
    )

    assert result.expiry is None


def test_extra_defaults_to_none():
    result = Result(
        name="Parcels",
        data={},
        task_name="LoadParcels",
    )

    assert result.extra is None


def test_datetime_format_stored():
    result = Result(
        name="Parcels",
        data={},
        task_name="LoadParcels",
    )

    assert result._datetime_format == "%Y-%m-%dT%H:%M:%S.%f"


# ============================================================================
# Equality
# ============================================================================

def test_equal_results_compare_true():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        name="Parcels",
        data={"a": 1},
        task_name="LoadParcels",
        last_updated=timestamp,
        extra={"x": 1},
    )

    right = Result(
        name="Parcels",
        data={"a": 1},
        task_name="LoadParcels",
        last_updated=timestamp,
        extra={"x": 1},
    )

    assert left == right


def test_results_with_different_name_not_equal():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        "One",
        data=1,
        task_name="Task",
        last_updated=timestamp,
    )

    right = Result(
        "Two",
        data=1,
        task_name="Task",
        last_updated=timestamp,
    )

    assert left != right


def test_results_with_different_task_name_not_equal():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        "Data",
        data=1,
        task_name="TaskA",
        last_updated=timestamp,
    )

    right = Result(
        "Data",
        data=1,
        task_name="TaskB",
        last_updated=timestamp,
    )

    assert left != right


def test_results_with_different_data_not_equal():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=timestamp,
    )

    right = Result(
        "Data",
        data=2,
        task_name="Task",
        last_updated=timestamp,
    )

    assert left != right


def test_results_with_different_dtype_not_equal():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        "Data",
        data=1,
        task_name="Task",
        dtype="int",
        last_updated=timestamp,
    )

    right = Result(
        "Data",
        data=1,
        task_name="Task",
        dtype="float",
        last_updated=timestamp,
    )

    assert left != right


def test_results_with_different_timestamp_not_equal():
    left = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=dt.datetime(2025, 1, 1),
    )

    right = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=dt.datetime(2025, 1, 2),
    )

    assert left != right


def test_results_with_different_expiry_not_equal():
    left = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=dt.datetime(2025, 1, 1),
        expiry=dt.datetime(2025, 2, 1),
    )

    right = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=dt.datetime(2025, 1, 1),
        expiry=None,
    )

    assert left != right


def test_results_with_different_extra_not_equal():
    timestamp = dt.datetime(2025, 1, 1)

    left = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=timestamp,
        extra={"a": 1},
    )

    right = Result(
        "Data",
        data=1,
        task_name="Task",
        last_updated=timestamp,
        extra={"a": 2},
    )

    assert left != right


def test_equality_with_non_result_returns_notimplemented():
    result = Result(
        "Data",
        data=1,
        task_name="Task",
    )

    assert result.__eq__(object()) is NotImplemented


# ============================================================================
# Repr
# ============================================================================

def test_repr_with_builtin_type():
    result = Result(
        name="Parcels",
        data={},
        task_name="Task",
    )

    assert repr(result) == "<Result 'Parcels' (dict)>"


def test_repr_with_custom_dtype():
    result = Result(
        name="Parcels",
        data={},
        task_name="Task",
        dtype="GeoDataFrame",
    )

    assert repr(result) == "<Result 'Parcels' (GeoDataFrame)>"
