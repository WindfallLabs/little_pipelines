"""Expiry functions."""

import atexit
import calendar
import datetime as dt
from typing import Callable

from ._cache import _ACTIVE_CACHE
from ._logger import app_logger



_on_complete_deletions: list[str] = []

# _callbacks: dict[str, list[str]] = {
#     "on_complete": [],
#     "on_fail": []
# }


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


def _del_cache_key(key):
    """Deletes results from the cache."""
    try:
        _ACTIVE_CACHE.delete(key)
    except Exception as e:
        pass
    return


def never() -> Callable:
    """Sets the data to never expire."""
    def expire_never() -> None:
        return None
    return expire_never


def at_midnight() -> Callable:
    """Sets the expiry to the next midnight."""
    def expire_at_midnight() -> int:
        now = dt.datetime.now()
        tomorrow = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_until = int((tomorrow - now).total_seconds())
        app_logger.log("{name} " + f"Expiry (seconds): {seconds_until}", "DEBUG")
        return seconds_until
    return expire_at_midnight


def from_now(years=0, months=0, weeks=0, days=0, hours=0, minutes=0, seconds=0) -> Callable:
    """Sets the expiry to a future time delta (from right now)."""
    def expire_from_now() -> int:
        # Calc the delta
        delta = dt.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        now = dt.datetime.now()
        new_year = now.year + years
        future_date = _add_months(now, months).replace(year=new_year, microsecond=0) + delta
        seconds_until = int((future_date - now).total_seconds())
        app_logger.log("{name} " + f"Expiry (seconds): {seconds_until}", "DEBUG")
        return seconds_until
    return expire_from_now


def from_today(years=0, months=0, weeks=0, days=0, hours=0, minutes=0, seconds=0) -> Callable:
    """Sets the expiry to a future time delta (from the past midnight)."""
    def expire_from_today() -> int:
        # Calc the delta
        delta = dt.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        # Get today (as datetime)
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        new_year = today.year + years
        future_date = _add_months(today, months).replace(year=new_year, microsecond=0) + delta
        seconds_until = int((future_date - today).total_seconds())
        app_logger.log("{name} " + f"Expiry (seconds): {seconds_until}", "DEBUG")
        return seconds_until
    return expire_from_today


def at_datetime(future_date: dt.date|dt.datetime) -> Callable:
    """Sets the expiry to a specific datetime.
    
    Use:
        expiry = at_datetime(dt.date(2025, 10, 31))
        seconds = expiry()
    """
    # Handle date input
    if isinstance(future_date, dt.date):
        future_date = dt.datetime(future_date.year, future_date.month, future_date.day)

    def expire_at_datetime() -> int:
        now = dt.datetime.now()
        seconds_until = int((future_date - now).total_seconds())
        app_logger.log("{name} " + f"Expiry (seconds): {seconds_until}", "DEBUG")
        return seconds_until

    return expire_at_datetime


def after_session(task_name: str):
    """Expires the given task results when Python exits."""
    atexit.register(lambda: _del_cache_key(task_name))
    def expire_at_session_end() -> None:
        return None  # Handled by atexit callback
    return expire_at_session_end


def on_complete(task_name: str):
    """Expires the results when the Pipeline completes."""
    _on_complete_deletions.append(task_name)
    def expire_on_pipeline_complete() -> None:
        return None  # Handled by pipeline
    return expire_on_pipeline_complete
