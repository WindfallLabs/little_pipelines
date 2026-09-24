"""
Test GeoDataFrame support
"""

import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from little_pipelines.shell.shell_utils._dataframes import (
    _handle_dataframe_printing_args,
)


@pytest.fixture
def fake_pandas(monkeypatch):
    pd = ModuleType("pandas")

    options = {
        "display.max_rows": 60,
        "display.max_columns": 20,
    }

    def get_option(name):
        return options[name]

    def set_option(name, value):
        options[name] = value

    pd.get_option = Mock(side_effect=get_option)
    pd.set_option = Mock(side_effect=set_option)

    monkeypatch.setitem(sys.modules, "pandas", pd)

    return pd, options


class GeoDataFrame:
    pass


GeoDataFrame.__module__ = "geopandas.geodataframe"


def test_geopandas_uses_pandas_settings(fake_pandas):
    _, options = fake_pandas

    reset = _handle_dataframe_printing_args(
        "--columns=5 --rows=10",
        GeoDataFrame(),
    )

    assert callable(reset)
    assert options["display.max_columns"] == 5
    assert options["display.max_rows"] == 10
