"""
Tests for expiry classes.

AI-generated tests (Claude Sonnet 4.6)
"""

import datetime as dt

import pytest

from little_pipelines import expiry  # Daily, Monthly, Weekly


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def monday_noon():
    """A Monday at noon — useful anchor for weekly/daily tests."""
    return dt.datetime(2025, 3, 3, 12, 0, 0)  # Monday


@pytest.fixture
def the_first():
    """The 1st of a month at noon."""
    return dt.datetime(2025, 3, 1, 12, 0, 0)


@pytest.fixture
def mid_month():
    """Mid-month datetime."""
    return dt.datetime(2025, 3, 10, 12, 0, 0)


# ---------------------------------------------------------------------------
# Daily
# ---------------------------------------------------------------------------

class TestDaily:
    def test_next_expiry_is_next_midnight(self, monday_noon):
        expires = expiry.Daily()
        result = expires.next_expiry(monday_noon)
        assert result == dt.datetime(2025, 3, 4, 0, 0, 0)

    def test_next_expiry_from_just_before_midnight(self):
        almost_midnight = dt.datetime(2025, 3, 3, 23, 59, 59)
        result = expiry.Daily().next_expiry(almost_midnight)
        assert result == dt.datetime(2025, 3, 4, 0, 0, 0)

    def test_is_stale_when_past_expiry(self, monday_noon):
        yesterday = dt.datetime.now() - dt.timedelta(days=1)
        assert expiry.Daily().is_stale(yesterday) is True

    def test_not_stale_when_before_expiry(self, monday_noon):
        tomorrow = dt.datetime.now() + dt.timedelta(days=1)
        assert expiry.Daily().is_stale(tomorrow) is False

    def test_time_remaining_is_positive_for_future(self, monday_noon):
        tomorrow = dt.datetime.now() + dt.timedelta(days=1)
        remaining = expiry.Daily().time_remaining(tomorrow)
        assert remaining > dt.timedelta(0)

    def test_time_remaining_none_for_no_expiry(self):
        assert expiry.Daily().time_remaining(None) is None


# ---------------------------------------------------------------------------
# Weekly
# ---------------------------------------------------------------------------

class TestWeekly:
    def test_next_expiry_finds_next_saturday(self, monday_noon):
        # Monday -> next Saturday
        expires = expiry.Weekly("Sat")
        result = expires.next_expiry(monday_noon)
        assert result == dt.datetime(2025, 3, 8, 0, 0, 0)

    def test_skips_current_day(self):
        # Saturday at noon -> next Saturday (not tonight)
        saturday_noon = dt.datetime(2025, 3, 8, 12, 0, 0)
        result = expiry.Weekly("Sat").next_expiry(saturday_noon)
        assert result == dt.datetime(2025, 3, 15, 0, 0, 0)

    def test_multiple_days_picks_soonest(self, monday_noon):
        # Monday -> Wednesday comes before Saturday
        expires = expiry.Weekly("Wed", "Sat")
        result = expires.next_expiry(monday_noon)
        assert result == dt.datetime(2025, 3, 5, 0, 0, 0)

    def test_invalid_day_raises_at_construction(self):
        with pytest.raises(ValueError):
            expiry.Weekly("Someday")

    def test_no_days_raises_at_construction(self):
        with pytest.raises(ValueError):
            expiry.Weekly()

    def test_case_insensitive(self, monday_noon):
        assert expiry.Weekly("saturday").next_expiry(monday_noon) == expiry.Weekly("SAT").next_expiry(monday_noon)


# ---------------------------------------------------------------------------
# Monthly
# ---------------------------------------------------------------------------

class TestMonthly:
    def test_default_expires_on_first(self, mid_month):
        # Mid-March -> 1st April
        result = expiry.Monthly().next_expiry(mid_month)
        assert result == dt.datetime(2025, 4, 1, 0, 0, 0)

    def test_expires_on_upcoming_day_same_month(self, mid_month):
        # March 10 -> March 15
        result = expiry.Monthly(15).next_expiry(mid_month)
        assert result == dt.datetime(2025, 3, 15, 0, 0, 0)

    def test_skips_current_day(self, the_first):
        # On the 1st -> next 1st (April)
        result = expiry.Monthly(1).next_expiry(the_first)
        assert result == dt.datetime(2025, 4, 1, 0, 0, 0)

    def test_multiple_days_picks_soonest(self, mid_month):
        # March 10 -> March 15 is sooner than April 1
        result = expiry.Monthly(1, 15).next_expiry(mid_month)
        assert result == dt.datetime(2025, 3, 15, 0, 0, 0)

    def test_wraps_to_next_month(self):
        late_march = dt.datetime(2025, 3, 20, 12, 0, 0)
        result = expiry.Monthly(10).next_expiry(late_march)
        assert result == dt.datetime(2025, 4, 10, 0, 0, 0)

    def test_day_out_of_range_raises(self):
        with pytest.raises(ValueError):
            expiry.Monthly(29)

    def test_default_is_first_of_month(self):
        assert expiry.Monthly().days == (1,)
