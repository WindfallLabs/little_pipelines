"""
Expires

Functions to build future datetimes based on now/today.
"""

# Improved by Claude (Sonnet 4.6)
import datetime as dt
from typing import Literal


_DAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

DayNames = Literal["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def never() -> dt.datetime:
    """Return a far-future datetime.
    Useful when data should never expire (waits until manually cleared).
    """
    return dt.datetime(9999, 12, 31, 11, 59)


def _to_datetime(d: dt.date | dt.datetime) -> dt.datetime:
    """Normalize a date or datetime to datetime, safely handling subclass overlap."""
    if type(d) is dt.date:
        return dt.datetime(d.year, d.month, d.day, 0, 0)
    if isinstance(d, dt.datetime):
        return d
    raise TypeError(f"Expected dt.date or dt.datetime, got {type(d).__name__}")


def nearest(*dates: dt.date | dt.datetime, now: dt.datetime | dt.date = None) -> dt.datetime:
    """Return the nearest date or datetime."""
    normalized = [_to_datetime(d) for d in dates]
    now = _to_datetime(now) if now is not None else dt.datetime.now()
    return min([d for d in normalized if d > now])


def daily(*times: str, now: dt.datetime = None) -> dt.datetime:
    """Creates a datetime object for the next specified time (e.g. '2:00 PM')."""
    now = now or dt.datetime.now()
    candidates = []
    for t in times:
        parsed_time = dt.datetime.strptime(t, "%I:%M %p").time()
        candidate = dt.datetime.combine(now.date(), parsed_time)
        if candidate <= now:
            candidate += dt.timedelta(days=1)
        candidates.append(candidate)
    return nearest(*candidates, now=now)


def weekly(*days: DayNames, today: dt.date = None) -> dt.datetime:
    """Creates a datetime object for the next specified weekday (e.g. 'Tue')."""
    today = today or dt.date.today()
    candidates = []
    for day in days:
        abbr = day[0:3].lower()
        delta = (_DAYS[abbr] - today.weekday()) % 7 or 7
        candidates.append(dt.datetime.combine(today + dt.timedelta(days=delta), dt.time.min))
    return nearest(*candidates, now=today)


def monthly(*days: int, today: dt.date = None) -> dt.datetime:
    """Creates a datetime object for the next specified day of the month (e.g. 15)."""
    today = today or dt.date.today()
    candidates = []
    for day in days:
        candidate = today.replace(day=day)
        if candidate <= today:
            month = today.month % 12 + 1
            year = today.year + (1 if today.month == 12 else 0)
            candidate = dt.date(year, month, day)
        candidates.append(dt.datetime.combine(candidate, dt.time.min))
    return nearest(*candidates, now=today)


def test():
    now = dt.datetime(2026, 3, 11, 11, 0)
    near = nearest(dt.date(2026, 4, 5), dt.datetime(2027, 6, 9, 4, 20), now=now)
    assert near == dt.datetime(2026, 4, 5)

    d1 = daily("11:30 PM", "8:00 AM", now=dt.datetime(2026, 3, 11, 23, 0))
    assert d1 == dt.datetime(2026, 3, 11, 23, 30)
    d2 = daily("11:30 PM", "8:00 AM", now=dt.datetime(2026, 3, 11, 23, 31))
    assert d2 == dt.datetime(2026, 3, 12, 8, 0)

    w1 = weekly("Friday", today=now)
    assert w1 == dt.datetime(2026, 3, 13)
    w2 = weekly("mon", "wed", today=now)
    assert w2 == dt.datetime(2026, 3, 16)

    w1 = monthly(1, today=now)
    assert w1 == dt.datetime(2026, 4, 1)
    w2 = monthly(1, 15, today=now)
    assert w2 == dt.datetime(2026, 3, 15)
