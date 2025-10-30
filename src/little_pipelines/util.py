"""Utils"""

import datetime as dt


def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg
