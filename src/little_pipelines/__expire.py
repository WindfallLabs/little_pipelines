"""Expiry functions."""

import atexit
import calendar
import datetime as dt
from typing import Callable, Optional

from ._cache import _ACTIVE_CACHE

_on_complete_deletions: list[str] = []


def _get_now() -> dt.datetime:
    """Centralized function to get current time. Override in tests via monkeypatch."""
    return dt.datetime.now()


def _add_months(date: dt.datetime, months: int) -> dt.datetime:
    """Add months to a datetime, handling month overflow correctly."""
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(date.day, last_day)
    return date.replace(year=year, month=month, day=day)


def _to_datetime(date: dt.date | dt.datetime) -> dt.datetime:
    """Convert date to datetime consistently."""
    if isinstance(date, dt.datetime):
        return date
    return dt.datetime(date.year, date.month, date.day)


def _get_midnight(base: dt.datetime) -> dt.datetime:
    """Get midnight (00:00:00) for a given datetime."""
    return base.replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_weekday(day_str: str) -> int:
    """
    Parse a weekday string to a weekday integer (e.g. 0=Monday, 6=Sunday).

    Args:
        day_str: Weekday name (e.g., "Mon", "monday", "Th", "Thursday")
    """
    day_str_lower = day_str.strip().lower()[:2]

    weekdays = [
        "mo", "tu", "we", "th", "fr", "sa", "su",
    ]

    if day_str_lower in weekdays:
        return weekdays.index(day_str_lower)

    raise ValueError(f"Unrecognized weekday: {day_str}")


def _get_next_weekday(
    weekdays: tuple[int, ...],
    today: dt.datetime
) -> dt.datetime:
    """
    Gets the datetime for the next occurring weekday.

    Args:
        weekdays: Tuple of weekday integers (0=Monday, 6=Sunday).
        today: Reference date to calculate from.

    Returns:
        The next occurrence of any of the specified weekdays at midnight.
    """
    if not weekdays:
        raise ValueError("Must specify at least one weekday")

    today_midnight = _get_midnight(today)
    current_weekday = today.weekday()

    candidates = []

    for target_weekday in weekdays:
        # Calculate days until this weekday
        days_ahead = (target_weekday - current_weekday) % 7

        # If days_ahead is 0, we're on that day - skip to next week
        if days_ahead == 0:
            days_ahead = 7

        candidate = today_midnight + dt.timedelta(days=days_ahead)
        candidates.append(candidate)

    return min(candidates)


def _seconds_until(target: dt.datetime, from_time: Optional[dt.datetime] = None) -> int:
    """Calculate seconds from now (or from_time) until target datetime."""
    now = from_time if from_time is not None else _get_now()
    return int((target - now).total_seconds())


def _get_next_month_day(
    days: tuple[int, ...],
    today: dt.datetime
) -> dt.datetime:
    """
    Gets the datetime for the next occurring month date.

    Args:
        days: The month-days to expire results on (e.g., (1,) for 1st of month).
        today: Reference date to calculate from.
    """
    candidates = []
    today_midnight = _get_midnight(today)

    for day in days:
        # Try current month
        try:
            candidate = dt.datetime(today.year, today.month, day)
            # Only include if it's strictly in the future (not today at midnight)
            if candidate > today_midnight:
                candidates.append(candidate)
        except ValueError:
            pass  # Invalid day for current month

        # Try next month
        next_month_date = _add_months(today, 1)
        try:
            candidate = dt.datetime(next_month_date.year, next_month_date.month, day)
            candidates.append(candidate)
        except ValueError:
            pass  # Invalid day for next month

    if not candidates:
        raise ValueError(f"No valid dates found for days {days}")

    return min(candidates)


def _del_cache_key(key: str) -> None:
    """Deletes results from the cache."""
    try:
        _ACTIVE_CACHE.delete(key)
    except Exception:
        pass  # Silently ignore cache deletion errors


def never() -> Callable[[], None]:
    """Sets the data to never expire."""
    def expire_never() -> None:
        return None
    return expire_never


def at_midnight() -> Callable[[], int]:
    """Sets the expiry to the next midnight."""
    def expire_at_midnight() -> int:
        now = _get_now()
        tomorrow = _get_midnight(now + dt.timedelta(days=1))
        return _seconds_until(tomorrow, now)
    return expire_at_midnight


def monthly(
    days: tuple[int, ...] = (1,)
) -> Callable[[], int]:
    """
    Sets the expiry to the next specified day of the month.

    Args:
        days: The month-days to expire results on. Defaults to (1,) for 1st of month.
    """
    def expire_monthly() -> int:
        today = _get_now()
        future_date = _get_next_month_day(days, today)
        return _seconds_until(future_date, today)

    return expire_monthly


def weekly(
    days: tuple[str, ...] = ("monday",)
) -> Callable[[], int]:
    """
    Sets the expiry to the next specified day of the week.

    Args:
        days: Tuple of weekday names. Accepts various formats:
              - Full names: "Monday", "Tuesday", etc.
              - 3-letter: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
              - 2-letter: "Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"
              Case insensitive. Defaults to ("monday",).

    Example:
        expire.weekly(days=("Mon", "Wed", "Fri"))
        expire.weekly(days=("thursday",))
    """
    # Parse weekday strings to integers once when function is created
    try:
        weekday_ints = tuple(_parse_weekday(day) for day in days)
    except ValueError as e:
        raise ValueError(f"Invalid weekday in weekly(): {e}")

    def expire_weekly() -> int:
        today = _get_now()
        future_date = _get_next_weekday(weekday_ints, today)
        return _seconds_until(future_date, today)

    return expire_weekly


def from_now(
    years: int = 0,
    months: int = 0,
    weeks: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> Callable[[], int]:
    """Sets the expiry to a future time delta from the moment of calling."""
    def expire_from_now() -> int:
        now = _get_now()
        future_date = now.replace(year=now.year + years)

        if months:
            future_date = _add_months(future_date, months)

        delta = dt.timedelta(weeks=weeks, days=days, hours=hours, 
                            minutes=minutes, seconds=seconds)
        future_date += delta

        return _seconds_until(future_date, now)

    return expire_from_now


def from_today(
    years: int = 0,
    months: int = 0,
    weeks: int = 0,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> Callable[[], int]:
    """Sets the expiry to a future time delta from today's midnight."""
    def expire_from_today() -> int:
        now = _get_now()
        today_midnight = _get_midnight(now)
        future_date = today_midnight.replace(year=today_midnight.year + years)

        if months:
            future_date = _add_months(future_date, months)

        delta = dt.timedelta(weeks=weeks, days=days, hours=hours,
                            minutes=minutes, seconds=seconds)
        future_date += delta

        return _seconds_until(future_date, now)

    return expire_from_today


def at_datetime(target_date: dt.date | dt.datetime) -> Callable[[], int]:
    """
    Sets the expiry to a specific datetime.

    Example:
        expiry = at_datetime(dt.date(2025, 10, 31))
        seconds = expiry()
    """
    target = _to_datetime(target_date)

    def expire_at_datetime() -> int:
        return _seconds_until(target)

    return expire_at_datetime


def after_session(task_name: str) -> Callable[[], None]:
    """Expires the given task results when Python exits."""
    atexit.register(lambda: _del_cache_key(task_name))

    def expire_at_session_end() -> None:
        return None

    return expire_at_session_end


def on_complete(task_name: str) -> Callable[[], None]:
    """Expires the results when the Pipeline completes."""
    _on_complete_deletions.append(task_name)

    def expire_on_pipeline_complete() -> None:
        return None

    return expire_on_pipeline_complete
