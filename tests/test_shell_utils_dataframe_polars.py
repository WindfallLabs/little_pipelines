"""
Test polars.Dataframe support
"""

import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from little_pipelines.shell.shell_utils._dataframes import (
    _handle_dataframe_printing_args,
)


class DataFrame:
    pass


DataFrame.__module__ = "polars.dataframe.frame"


@pytest.fixture
def fake_polars(monkeypatch):
    pl = ModuleType("polars")

    class Config:
        set_tbl_cols = Mock()
        set_tbl_rows = Mock()
        restore_defaults = Mock()

    pl.Config = Config

    monkeypatch.setitem(sys.modules, "polars", pl)

    return pl


def test_sets_polars_config(fake_polars):
    reset = _handle_dataframe_printing_args(
        "--columns=3 --rows=4",
        DataFrame(),
    )

    fake_polars.Config.set_tbl_cols.assert_called_once_with(3)
    fake_polars.Config.set_tbl_rows.assert_called_once_with(4)

    assert reset is fake_polars.Config.restore_defaults
