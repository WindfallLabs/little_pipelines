"""
Utils
"""

import datetime as dt
from pathlib import Path


HOME = Path().home() / ".little_pipelines"
DEFAULT = HOME / "DEFAULT"
DEFAULT_LOG_DIR = DEFAULT / "logs"
DEFAULT_CACHE_FILE = DEFAULT / "DEFAULT_CACHE"
if not DEFAULT.exists():
    DEFAULT.mkdir()


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg
