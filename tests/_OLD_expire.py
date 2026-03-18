"""Tests for result expiration functionality."""

import datetime as dt
from unittest.mock import patch

import pytest
from little_pipelines import expire


class TestHelpers:
    """Test internal helper functions."""

    def test_add_months_basic(self):
        result = expire._add_months(dt.datetime(2025, 10, 1), 2)
        assert result == dt.datetime(2025, 12, 1)

    def test_add_months_year_overflow(self):
        result = expire._add_months(dt.datetime(2025, 10, 1), 13)
        assert result == dt.datetime(2026, 11, 1)

    def test_add_months_day_overflow(self):
        # Oct 31 + 4 months = Feb 28 (day capped to month's last day)
        result = expire._add_months(dt.datetime(2025, 10, 31), 4)
        assert result == dt.datetime(2026, 2, 28)

    def test_add_months_zero(self):
        result = expire._add_months(dt.datetime(2025, 10, 1), 0)
        assert result == dt.datetime(2025, 10, 1)

    def test_to_datetime_with_datetime(self):
        dt_input = dt.datetime(2025, 10, 31, 14, 30)
        result = expire._to_datetime(dt_input)
        assert result == dt_input

    def test_to_datetime_with_date(self):
        date_input = dt.date(2025, 10, 31)
        result = expire._to_datetime(date_input)
        assert result == dt.datetime(2025, 10, 31)

    def test_get_midnight(self):
        dt_input = dt.datetime(2025, 10, 31, 14, 30, 45, 123456)
        result = expire._get_midnight(dt_input)
        assert result == dt.datetime(2025, 10, 31, 0, 0, 0, 0)

    def test_seconds_until(self):
        now = dt.datetime(2025, 10, 31, 10, 0, 0)
        target = dt.datetime(2025, 10, 31, 11, 0, 0)
        result = expire._seconds_until(target, now)
        assert result == 3600

    def test_seconds_until_uses_now_by_default(self):
        target = dt.datetime(2025, 10, 31, 11, 0, 0)
        with patch('little_pipelines.expire._get_now', return_value=dt.datetime(2025, 10, 31, 10, 0, 0)):
            result = expire._seconds_until(target)
            assert result == 3600


class TestNever:
    def test_never_returns_none(self):
        assert expire.never()() is None


class TestAtMidnight:
    def test_at_midnight(self):
        mock_now = dt.datetime(2025, 10, 31, 14, 30, 45)
        expected_midnight = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((expected_midnight - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_midnight()
            assert calc_seconds() == expected_seconds

    def test_at_midnight_before_noon(self):
        mock_now = dt.datetime(2025, 10, 31, 8, 15, 30)
        expected_midnight = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((expected_midnight - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_midnight()
            assert calc_seconds() == expected_seconds


class TestAtDatetime:
    def test_at_datetime_with_datetime(self):
        mock_now = dt.datetime(2025, 10, 31, 14, 30)
        target = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((target - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_datetime(target)
            assert calc_seconds() == expected_seconds

    def test_at_datetime_with_date(self):
        mock_now = dt.datetime(2025, 10, 31, 14, 30)
        target_date = dt.date(2025, 11, 1)
        target_datetime = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((target_datetime - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_datetime(target_date)
            assert calc_seconds() == expected_seconds


class TestMonthly:
    def test_monthly_first_of_month_current_month(self):
        # Currently Oct 15, next 1st is Nov 1
        mock_today = dt.datetime(2025, 10, 15, 10, 0)
        expected_target = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly()
            assert calc_seconds() == expected_seconds

    def test_monthly_first_of_month_next_month(self):
        # Currently Oct 15, next 1st is Nov 1
        mock_today = dt.datetime(2025, 10, 15, 10, 0)
        expected_target = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly(days=(1,))
            assert calc_seconds() == expected_seconds

    def test_monthly_multiple_days(self):
        # Oct 31, next occurrence from (15, 25) should be Nov 15
        mock_today = dt.datetime(2025, 10, 31, 10, 0)
        expected_target = dt.datetime(2025, 11, 15, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly(days=(15, 25))
            assert calc_seconds() == expected_seconds

    def test_monthly_invalid_day(self):
        # Feb doesn't have 31st, should skip to next valid day
        mock_today = dt.datetime(2025, 2, 15, 10, 0)
        # Should find March 31 as next valid occurrence
        expected_target = dt.datetime(2025, 3, 31, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly(days=(31,))
            assert calc_seconds() == expected_seconds


class TestWeekly:
    """Test weekly expiration functionality."""

    def test_weekly_default_monday(self):
        # Friday Oct 31, 2025 -> next Monday is Nov 3
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly()
            assert calc_seconds() == expected_seconds

    def test_weekly_specific_day(self):
        # Friday Oct 31 -> next Wednesday is Nov 5
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 5, 0, 0, 0)  # Wednesday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Wednesday",))
            assert calc_seconds() == expected_seconds

    def test_weekly_multiple_days(self):
        # Friday Oct 31 -> next occurrence from (Mon, Wed, Fri) should be Mon Nov 3
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday (closest)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Monday", "Wednesday", "Friday"))
            assert calc_seconds() == expected_seconds

    def test_weekly_on_target_day(self):
        # Monday Nov 3 at midnight -> should skip to next Monday Nov 10
        mock_today = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday at midnight
        expected_target = dt.datetime(2025, 11, 10, 0, 0, 0)  # Next Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Monday",))
            assert calc_seconds() == expected_seconds

    def test_weekly_later_on_target_day(self):
        # Monday Nov 3 at 11 PM -> should skip to next Monday Nov 10
        mock_today = dt.datetime(2025, 11, 3, 23, 0, 0)  # Monday at 11 PM
        expected_target = dt.datetime(2025, 11, 10, 0, 0, 0)  # Next Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Monday",))
            assert calc_seconds() == expected_seconds

    def test_weekly_three_letter_abbreviation(self):
        # Test "Mon", "Tue", etc.
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Mon",))
            assert calc_seconds() == expected_seconds

    def test_weekly_two_letter_abbreviation(self):
        # Test "Mo", "Tu", etc.
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 4, 0, 0, 0)  # Tuesday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Tu",))
            assert calc_seconds() == expected_seconds

    def test_weekly_case_insensitive(self):
        # Test various case formats
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 6, 0, 0, 0)  # Thursday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("THURSDAY",))
            assert calc_seconds() == expected_seconds

            calc_seconds = expire.weekly(days=("thursday",))
            assert calc_seconds() == expected_seconds

            calc_seconds = expire.weekly(days=("Thursday",))
            assert calc_seconds() == expected_seconds

    def test_weekly_with_whitespace(self):
        # Test that whitespace is handled
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("  Monday  ",))
            assert calc_seconds() == expected_seconds

    def test_weekly_all_days_of_week(self):
        # Test each day of the week
        # Starting on Friday Oct 31, 2025
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday

        expected_days = {
            "Saturday": dt.datetime(2025, 11, 1, 0, 0, 0),   # Tomorrow
            "Sunday": dt.datetime(2025, 11, 2, 0, 0, 0),     # Day after
            "Monday": dt.datetime(2025, 11, 3, 0, 0, 0),     # 3 days
            "Tuesday": dt.datetime(2025, 11, 4, 0, 0, 0),    # 4 days
            "Wednesday": dt.datetime(2025, 11, 5, 0, 0, 0),  # 5 days
            "Thursday": dt.datetime(2025, 11, 6, 0, 0, 0),   # 6 days
            "Friday": dt.datetime(2025, 11, 7, 0, 0, 0),     # Next week
        }

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            for day_name, expected_target in expected_days.items():
                calc_seconds = expire.weekly(days=(day_name,))
                expected_seconds = int((expected_target - mock_today).total_seconds())
                assert calc_seconds() == expected_seconds, f"Failed for {day_name}"

    def test_weekly_with_today_override(self):
        # Test consistent behavior with mocked time
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        expected_target = dt.datetime(2025, 11, 3, 0, 0, 0)  # Monday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Monday",))
            assert calc_seconds() == expected_seconds

    def test_weekly_invalid_day_name(self):
        # Test that invalid day names raise errors

        with pytest.raises(ValueError, match="Invalid weekday"):
            expire.weekly(days=("Mon", "NotADay"))

    def test_weekly_empty_days_tuple(self):
        # Test that empty tuple raises error
        with pytest.raises(ValueError, match="Must specify at least one weekday"):
            calc_seconds = expire.weekly(days=())
            calc_seconds()

    def test_weekly_mixed_formats(self):
        # Test mixing full names and abbreviations
        mock_today = dt.datetime(2025, 10, 31, 10, 0)  # Friday
        # Should pick Saturday (tomorrow) as earliest
        expected_target = dt.datetime(2025, 11, 1, 0, 0, 0)  # Saturday
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.weekly(days=("Monday", "Sat", "We"))
            assert calc_seconds() == expected_seconds


class TestWeekdayParsing:
    """Test the _parse_weekday helper function."""

    def test_parse_full_names(self):
        assert expire._parse_weekday("Monday") == 0
        assert expire._parse_weekday("Tuesday") == 1
        assert expire._parse_weekday("Wednesday") == 2
        assert expire._parse_weekday("Thursday") == 3
        assert expire._parse_weekday("Friday") == 4
        assert expire._parse_weekday("Saturday") == 5
        assert expire._parse_weekday("Sunday") == 6

    def test_parse_case_insensitive(self):
        assert expire._parse_weekday("MONDAY") == 0
        assert expire._parse_weekday("monday") == 0
        assert expire._parse_weekday("MoNdAy") == 0

    def test_parse_with_whitespace(self):
        assert expire._parse_weekday("  Monday  ") == 0
        assert expire._parse_weekday("\tTuesday\n") == 1

    def test_parse_invalid_day(self):
        with pytest.raises(ValueError, match="Unrecognized weekday"):
            expire._parse_weekday("NotADay")

        with pytest.raises(ValueError, match="Unrecognized weekday"):
            expire._parse_weekday("")


class TestFromNow:
    def test_from_now_seconds(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(seconds=1)
            assert calc_seconds() == 1

    def test_from_now_minutes(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(minutes=1)
            assert calc_seconds() == 60

    def test_from_now_hours(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(hours=1)
            assert calc_seconds() == 3600

    def test_from_now_days(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(days=1)
            assert calc_seconds() == 86400

    def test_from_now_weeks(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(weeks=1)
            assert calc_seconds() == 604800

    def test_from_now_months(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        # Oct 31 + 1 month = Nov 30 (capped)
        expected = dt.datetime(2025, 11, 30, 10, 0, 0)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(months=1)
            assert calc_seconds() == expected_seconds

    def test_from_now_years(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        expected = dt.datetime(2026, 10, 31, 10, 0, 0)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(years=1)
            assert calc_seconds() == expected_seconds

    def test_from_now_combined(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        # 1 year, 2 months, 3 days, 4 hours
        expected = dt.datetime(2026, 10, 31, 10, 0, 0)
        expected = expire._add_months(expected, 2)
        expected += dt.timedelta(days=3, hours=4)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(years=1, months=2, days=3, hours=4)
            assert calc_seconds() == expected_seconds


class TestFromToday:
    def test_from_today_seconds(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight + dt.timedelta(seconds=1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(seconds=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_minutes(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight + dt.timedelta(minutes=1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(minutes=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_hours(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight + dt.timedelta(hours=1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(hours=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_days(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight + dt.timedelta(days=1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(days=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_weeks(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight + dt.timedelta(weeks=1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(weeks=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_months(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = expire._add_months(midnight, 1)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(months=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_years(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight.replace(year=2026)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(years=1)
            assert calc_seconds() == expected_seconds

    def test_from_today_combined(self):
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 45)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        expected = midnight.replace(year=2026)
        expected = expire._add_months(expected, 2)
        expected += dt.timedelta(days=3, hours=4)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today(years=1, months=2, days=3, hours=4)
            assert calc_seconds() == expected_seconds


class TestOnComplete:
    def test_on_complete_adds_to_list(self):
        # Clear the list first
        expire._on_complete_deletions.clear()

        func = expire.on_complete("TEST_TASK")
        assert func() is None
        assert "TEST_TASK" in expire._on_complete_deletions

        # Cleanup
        expire._on_complete_deletions.clear()

    def test_on_complete_multiple_tasks(self):
        expire._on_complete_deletions.clear()

        expire.on_complete("TASK1")
        expire.on_complete("TASK2")
        expire.on_complete("TASK3")

        assert len(expire._on_complete_deletions) == 3
        assert "TASK1" in expire._on_complete_deletions
        assert "TASK2" in expire._on_complete_deletions
        assert "TASK3" in expire._on_complete_deletions

        expire._on_complete_deletions.clear()


class TestAfterSession:
    def test_after_session_returns_none(self):
        func = expire.after_session("TEST_SESSION")
        assert func() is None


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_negative_seconds_past_datetime(self):
        """Test what happens when target datetime is in the past."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        past_target = dt.datetime(2025, 10, 30, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_datetime(past_target)
            # Should return negative seconds
            assert calc_seconds() == -86400

    def test_leap_year_handling(self):
        """Test month addition handles leap years correctly."""
        # 2024 is a leap year, Feb has 29 days
        leap_jan_31 = dt.datetime(2024, 1, 31)
        result = expire._add_months(leap_jan_31, 1)
        assert result == dt.datetime(2024, 2, 29)

        # 2025 is not a leap year, Feb has 28 days
        non_leap_jan_31 = dt.datetime(2025, 1, 31)
        result = expire._add_months(non_leap_jan_31, 1)
        assert result == dt.datetime(2025, 2, 28)

    def test_add_negative_months(self):
        """Test subtracting months (negative values)."""
        result = expire._add_months(dt.datetime(2025, 3, 31), -2)
        assert result == dt.datetime(2025, 1, 31)

        # Cross year boundary
        result = expire._add_months(dt.datetime(2025, 2, 15), -3)
        assert result == dt.datetime(2024, 11, 15)

    def test_monthly_on_current_day(self):
        """Test monthly when today is exactly the target day."""
        # It's Nov 1st at midnight - should target Dec 1st, not today
        mock_today = dt.datetime(2025, 11, 1, 0, 0, 0)
        expected_target = dt.datetime(2025, 12, 1, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly(days=(1,))
            assert calc_seconds() == expected_seconds

    def test_monthly_later_in_current_day(self):
        """Test monthly when it's the target day but later in the day."""
        # It's Nov 1st at 11 PM - should still target Dec 1st
        mock_today = dt.datetime(2025, 11, 1, 23, 0, 0)
        expected_target = dt.datetime(2025, 12, 1, 0, 0, 0)
        expected_seconds = int((expected_target - mock_today).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            calc_seconds = expire.monthly(days=(1,))
            assert calc_seconds() == expected_seconds

    def test_monthly_december_rollover(self):
        """Test monthly rolls over to next year correctly."""
        # Dec 15, next occurrence should be Jan 1 of next year
        expected_target = dt.datetime(2026, 1, 1, 0, 0, 0)
        expected_seconds = int((expected_target - dt.datetime(2025, 12, 15)).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=dt.datetime(2025, 12, 15)):
            calc_seconds = expire.monthly(days=(1,))
            assert calc_seconds() == expected_seconds

    def test_monthly_no_valid_days_error(self):
        """Test monthly raises error when no valid days can be found."""
        # Day 32 is never valid
        mock_today = dt.datetime(2025, 1, 15, 10, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_today):
            with pytest.raises(ValueError, match="No valid dates found"):
                calc_seconds = expire.monthly(days=(32, 33))
                calc_seconds()

    def test_from_now_zero_values(self):
        """Test from_now with all zeros returns 0 seconds."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now()
            assert calc_seconds() == 0

    def test_from_today_zero_values(self):
        """Test from_today with all zeros returns seconds until midnight."""
        mock_now = dt.datetime(2025, 10, 31, 10, 30, 0)
        midnight = dt.datetime(2025, 10, 31, 0, 0, 0)
        # Should be negative (midnight was 10.5 hours ago)
        expected_seconds = int((midnight - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_today()
            assert calc_seconds() == expected_seconds

    def test_very_large_time_deltas(self):
        """Test with very large time periods (100 years)."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        expected = dt.datetime(2125, 10, 31, 10, 0, 0)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(years=100)
            assert calc_seconds() == expected_seconds

    def test_microsecond_precision(self):
        """Test that microseconds are properly handled."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0, 500000)  # 0.5 seconds
        target = dt.datetime(2025, 10, 31, 10, 0, 1, 0)  # 1 second, 0 microseconds
        # Difference should be 0.5 seconds = 0 when converted to int

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.at_datetime(target)
            # int() truncates, so 0.5 seconds becomes 0
            assert calc_seconds() == 0

    def test_year_month_interaction(self):
        """Test that years and months interact correctly."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)
        # 1 year + 3 months = Oct 2026 + 3 months = Jan 2027
        # But Oct 31 + 3 months = Jan 31 2027
        expected = dt.datetime(2026, 10, 31, 10, 0, 0)
        expected = expire._add_months(expected, 3)
        expected_seconds = int((expected - mock_now).total_seconds())

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            calc_seconds = expire.from_now(years=1, months=3)
            assert calc_seconds() == expected_seconds

    def test_dst_transitions(self):
        """Test behavior during DST transitions (documenting current behavior)."""
        # This test documents how the library handles DST.
        # In the US, DST typically ends first Sunday in November.
        # This test ensures we're aware of the behavior, not necessarily testing it's "correct"

        # Before DST ends (Oct 31, 2025 at 10 AM)
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            # 24 hours from now
            calc_seconds = expire.from_now(days=1)
            # Should be exactly 86400 seconds (timedelta doesn't account for DST)
            assert calc_seconds() == 86400

    def test_empty_days_tuple(self):
        """Test monthly with empty days tuple."""

        with patch('little_pipelines.expire._get_now', return_value=dt.datetime(2025, 10, 15)):
            with pytest.raises(ValueError, match="No valid dates found"):
                calc_seconds = expire.monthly(days=())
                calc_seconds()

    def test_closure_captures_correct_values(self):
        """Test that closures capture values correctly (not references)."""
        mock_now = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now):
            # Create multiple closures with different values
            expire_1h = expire.from_now(hours=1)
            expire_2h = expire.from_now(hours=2)
            expire_3h = expire.from_now(hours=3)

            # All should return different values
            assert expire_1h() == 3600
            assert expire_2h() == 7200
            assert expire_3h() == 10800

    def test_callable_reuse(self):
        """Test that calling the same closure multiple times works correctly."""
        # First call at 10 AM
        mock_now_1 = dt.datetime(2025, 10, 31, 10, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now_1):
            calc_seconds = expire.from_now(hours=1)
            result_1 = calc_seconds()
            assert result_1 == 3600

        # Second call at 11 AM (1 hour later)
        mock_now_2 = dt.datetime(2025, 10, 31, 11, 0, 0)

        with patch('little_pipelines.expire._get_now', return_value=mock_now_2):
            result_2 = calc_seconds()
            # Still 1 hour from "now" (which is now 11 AM)
            assert result_2 == 3600
