"""Expiry functions."""

import atexit
import calendar
import datetime as dt
from typing import Callable, Literal

from ._cache import get_cache


def _add_months(date: dt.date|dt.datetime, months) -> dt.datetime:
    # Handle date input
    if isinstance(date, dt.date):
        date = dt.datetime(date.year, date.month, date.day)
    # Calculate new month and year
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    # Get the last day of the target month
    last_day = calendar.monthrange(year, month)[1]
    # Use the minimum of original day and last day of new month
    day = min(date.day, last_day)
    return date.replace(year=year, month=month, day=day)


def never() -> Callable:
    """Sets the data to never expire."""
    def calc() -> None:
        return None
    return calc


# def now() -> Callable:  # NOTE: this ruins data access
#     """Sets the data to expire immediately."""
#     def calc() -> Literal[0]:
#         return 0
#     return calc


def at_midnight() -> Callable:
    """Sets the expiry to the next midnight."""
    def calc() -> int:
        now = dt.datetime.now()
        tomorrow = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_until = int((tomorrow - now).total_seconds())
        return seconds_until
    return calc


def from_now(years=0, months=0, weeks=0, days=0, hours=0, minutes=0, seconds=0) -> Callable:
    """Sets the expiry to a future time delta (from right now)."""
    def calc() -> int:
        # Calc the delta
        delta = dt.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        now = dt.datetime.now()
        new_year = now.year + years
        future_date = _add_months(now, months).replace(year=new_year, microsecond=0) + delta
        seconds_until = int((future_date - now).total_seconds())
        return seconds_until
    return calc


def from_today(years=0, months=0, weeks=0, days=0, hours=0, minutes=0, seconds=0) -> Callable:
    """Sets the expiry to a future time delta (from the past midnight)."""
    def calc() -> int:
        # Calc the delta
        delta = dt.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        # Get today (as datetime)
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        new_year = today.year + years
        future_date = _add_months(today, months).replace(year=new_year, microsecond=0) + delta
        seconds_until = int((future_date - today).total_seconds())
        return seconds_until
    return calc


def at_datetime(future_date: dt.date|dt.datetime) -> Callable:
    """Sets the expiry to a specific datetime.
    
    Use:
        expiry = at_datetime(dt.date(2025, 10, 31))
        seconds = expiry()
    """
    # Handle date input
    if isinstance(future_date, dt.date):
        future_date = dt.datetime(future_date.year, future_date.month, future_date.day)

    def calc() -> int:
        now = dt.datetime.now()
        seconds_until = int((future_date - now).total_seconds())
        return seconds_until

    return calc


def session(task_name: str):
    """Expires the given task results when Python exits."""
    atexit.register(lambda: get_cache().delete(task_name))
    def calc() -> None:
        return None  # Appears to be no expiration
    return calc
