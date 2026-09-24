"""
Test dataframe support
"""

from little_pipelines.shell.shell_utils._dataframes import (
    _handle_dataframe_printing_args,
)


def test_non_dataframe_returns_none():
    assert (
        _handle_dataframe_printing_args(
            "--columns=10 --rows=20",
            object(),
        )
        is None
    )
