"""
Shell Utils
"""

import re
from typing import Any, Callable, Optional


def _handle_dataframe_printing_args(inp: str, result: Any) -> Callable|None:
    """Handles setting and resetting column/row counts.

    Args:
        task_name: str
        --columns: int
        --rows: int

    Returns:
        None, or a function to reset dataframe printing to default.
    """
    result_type = str(type(result)).lower()

    if "dataframe" not in result_type:
        return None

    cols = re.findall(r"--columns=(\d+)", inp)
    rows = re.findall(r"--rows=(\d+)", inp)
    set_cols: Optional[Callable] = None
    set_rows: Optional[Callable] = None
    reset_func: Optional[Callable] = None

    if (cols or rows) and "dataframe" in result_type:
        if "polars" in result_type:
            import polars as pl
            set_cols = pl.Config.set_tbl_cols
            set_rows = pl.Config.set_tbl_rows
            reset_func = pl.Config.restore_defaults
        elif "pandas" in result_type:
            import pandas as pd
            default_rows: int = pd.get_option("display.max_rows"),
            default_cols: int = pd.get_option("display.max_columns")
            set_cols = lambda c: pd.set_option("display.max_columns", c)
            set_rows = lambda r: pd.set_option("display.max_rows", r)

            def reset_func():
                pd.set_option("display.max_columns", default_cols)
                pd.set_option("display.max_rows", default_rows)

        if cols:
            set_cols(int(cols[0]))
        if rows:
            set_rows(int(rows[0]))

    return reset_func
