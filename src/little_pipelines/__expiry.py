"""
Expiry

Composable expiry objects for use with ShelfCache.

Usage:
    from expiry import Daily, Weekly, Monthly

    # Expires at the next midnight
    expiry = Daily()

    # Expires next Saturday at midnight
    expiry = Weekly("Sat")

    # Expires on the next 1st of the month (default)
    expiry = Monthly()

    # Expires on the next 1st or 15th of the month
    expiry = Monthly(1, 15)

    # Compute the expiry datetime (called by ShelfCache.write)
    expires_at = expiry.next_expiry(from_date=dt.datetime.now())

    # Check staleness and time remaining (called by ShelfCache.get)
    expiry.is_stale(expires_at)        # -> bool
    expiry.time_remaining(expires_at)  # -> dt.timedelta | None
"""

import calendar
import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_midnight(base: dt.datetime) -> dt.datetime:
    """Return midnight (00:00:00) on the given date."""
    return base.replace(hour=0, minute=0, second=0, microsecond=0)


def _add_months(date: dt.datetime, months: int) -> dt.datetime:
    """Add months to a datetime, handling month-end overflow correctly."""
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(date.day, last_day)
    return date.replace(year=year, month=month, day=day)


_WEEKDAY_MAP: dict[str, int] = {
    "mo": 0, "tu": 1, "we": 2, "th": 3, "fr": 4, "sa": 5, "su": 6,
}


def _parse_weekday(day_str: str) -> int:
    """
    Parse a weekday string to an integer (0=Monday, 6=Sunday).

    Accepts full names ("Monday"), 3-letter ("Mon"), or 2-letter ("Mo").
    Case insensitive.
    """
    key = day_str.strip().lower()[:2]
    if key not in _WEEKDAY_MAP:
        raise ValueError(f"Unrecognized weekday: {day_str!r}")
    return _WEEKDAY_MAP[key]


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Expiry(ABC):
    """Abstract base for all expiry types."""

    @abstractmethod
    def next_expiry(self, from_date: dt.datetime) -> dt.datetime | None:
        """
        Compute the next expiry datetime relative to from_date.

        Always returns a datetime strictly in the future of from_date.
        Returns None for non-expiring types.
        """

    def is_stale(self, expires_at: dt.datetime | None) -> bool:
        """Return True if the cached data has passed its expiry."""
        if expires_at is None:
            return False
        return dt.datetime.now() >= expires_at

    def time_remaining(self, expires_at: dt.datetime | None) -> dt.timedelta | None:
        """
        Return the time remaining until expiry.

        Returns None if the data never expires.
        Returns a zero or negative timedelta if already stale.
        """
        if expires_at is None:
            return None
        return expires_at - dt.datetime.now()


# ---------------------------------------------------------------------------
# Expiry types
# ---------------------------------------------------------------------------

@dataclass
class Daily(Expiry):
    """
    Expires at the next midnight.

    Example:
        expiry = Daily()
        expires_at = expiry.next_expiry(dt.datetime.now())
    """

    def next_expiry(self, from_date: dt.datetime) -> dt.datetime:
        return _get_midnight(from_date + dt.timedelta(days=1))


@dataclass
class Weekly(Expiry):
    """
    Expires at midnight on the next occurrence of any of the given weekdays.

    Skips the current day — if today is Saturday and "Sat" is specified,
    expiry is set to the following Saturday.

    Args:
        *days: One or more weekday names. Accepts full names ("Monday"),
               3-letter ("Mon"), or 2-letter ("Mo"). Case insensitive.

    Examples:
        Weekly("Sat")
        Weekly("Mon", "Thu")
    """
    days: tuple[str, ...] = field(default_factory=lambda: ("Monday",))

    def __init__(self, *days: str):
        if not days:
            raise ValueError("Weekly requires at least one weekday.")
        # Validate all days up front and store as canonical integers
        self._weekday_ints: tuple[int, ...] = tuple(_parse_weekday(d) for d in days)
        self.days = days

    def next_expiry(self, from_date: dt.datetime) -> dt.datetime:
        today_midnight = _get_midnight(from_date)
        current_weekday = from_date.weekday()

        candidates = []
        for target in self._weekday_ints:
            days_ahead = (target - current_weekday) % 7
            if days_ahead == 0:
                days_ahead = 7  # skip current day, go to next week
            candidates.append(today_midnight + dt.timedelta(days=days_ahead))

        return min(candidates)


@dataclass
class Monthly(Expiry):
    """
    Expires at midnight on the next occurrence of any of the given month-days.

    Skips the current day — if today is the 1st and 1 is specified,
    expiry is set to the 1st of the following month.

    Args:
        *days: One or more month-day integers (1-28). Defaults to (1,).

    Examples:
        Monthly()          # expires on the 1st of each month
        Monthly(1, 15)     # expires on the 1st or 15th, whichever is sooner
    """
    days: tuple[int, ...] = field(default_factory=lambda: (1,))

    def __init__(self, *days: int):
        self.days = days if days else (1,)
        for d in self.days:
            if not (1 <= d <= 28):
                raise ValueError(
                    f"Day {d} is out of range. Use 1-28 to ensure validity across all months."
                )

    def next_expiry(self, from_date: dt.datetime) -> dt.datetime:
        today_midnight = _get_midnight(from_date)
        candidates = []

        for day in self.days:
            # Try current month — only valid if strictly after today
            try:
                candidate = dt.datetime(from_date.year, from_date.month, day)
                if candidate > today_midnight:
                    candidates.append(candidate)
            except ValueError:
                pass

            # Always try next month as a fallback
            next_month = _add_months(from_date, 1)
            try:
                candidates.append(dt.datetime(next_month.year, next_month.month, day))
            except ValueError:
                pass

        if not candidates:
            raise ValueError(f"Could not compute a valid next expiry for days={self.days}")

        return min(candidates)
