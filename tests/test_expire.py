"""Tests for result expiration functionality."""

import datetime as dt
from pathlib import Path

import pytest
from little_pipelines import expire


def test_add_months():
    result1 = expire._add_months(
        dt.date(2025, 10, 31),
        4
    )

    result2 = expire._add_months(
        dt.date(2025, 10, 1),
        2
    )

    result3 = expire._add_months(
        dt.date(2025, 10, 1),
        0
    )

    result4 = expire._add_months(
        dt.date(2025, 10, 1),
        13
    )

    assert result1 == dt.datetime(2026, 2, 28)
    assert result2 == dt.datetime(2025, 12, 1)
    assert result3 == dt.datetime(2025, 10, 1)
    assert result4 == dt.datetime(2026, 11, 1)


def test_never():
    assert expire.never()() == None


def test_midnight():
    now = dt.datetime.now()
    secs = expire.at_midnight()()
    midnight = (
        (now + dt.timedelta(days=1))
        .replace(hour=0, minute=0, second=0, microsecond=0)
    )
    assert secs == int((midnight - now).total_seconds())


def test_datetime():
    now = dt.datetime.now()
    tomorrow = (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    calc_seconds = expire.at_datetime(tomorrow)
    assert calc_seconds() == int((tomorrow - now).total_seconds())


def test_on_complete():
    func = expire.on_complete("TEST")
    assert func() == None
    assert "TEST" in expire._on_complete_deletions
    expire._on_complete_deletions = []


class TestFromNow():
    def test_from_now_year(self):
        now = dt.datetime.now()
        calc_seconds = expire.from_now(
            years=1
        )
        year_seconds = int(((now.replace(year=now.year+1)) - now).total_seconds())
        # Within a day
        assert year_seconds - 86400 < calc_seconds() < year_seconds + 86400

    def test_from_now_month(self):
        now = dt.datetime.now()
        calc_seconds = expire.from_now(
            months=1
        )
        # Within a couple days
        low = int(((now + dt.timedelta(days=28)) - now).total_seconds())
        high = int(((now + dt.timedelta(days=31)) - now).total_seconds())
        assert low <= calc_seconds() <= high

    def test_from_now_week(self):
        now = dt.datetime.now()
        calc_seconds = expire.from_now(
            weeks=1
        )
        week_seconds = int(((now + dt.timedelta(days=7)) - now).total_seconds())
        assert calc_seconds() == week_seconds

    def test_from_now_day(self):
        now = dt.datetime.now()
        calc_seconds = expire.from_now(
            days=1
        )
        est = int(((now + dt.timedelta(days=1)) - now).total_seconds())
        assert calc_seconds() == est

    def test_from_now_hour(self):
        calc_seconds = expire.from_now(
            hours=1
        )
        assert calc_seconds() == 3600

    def test_from_now_minutes(self):
        calc_seconds = expire.from_now(
            minutes=1
        )
        assert calc_seconds() == 60

    def test_from_now_seconds(self):
        calc_seconds = expire.from_now(
            seconds=1
        )
        assert calc_seconds() == 1


class TestFromToday():
    def test_from_today_year(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            years=1
        )
        assert calc_seconds() == (today.replace(year=today.year+1) - today).total_seconds()

    def test_from_today_month(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            months=1
        )
        # Within a couple days
        low = int(((today + dt.timedelta(days=28)) - today).total_seconds())
        high = int(((today + dt.timedelta(days=31)) - today).total_seconds())
        assert low <= calc_seconds() <= high

    def test_from_today_week(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            weeks=1
        )
        assert calc_seconds() == ((today + dt.timedelta(weeks=1)) - today).total_seconds()

    def test_from_today_day(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            days=1
        )
        assert calc_seconds() == ((today + dt.timedelta(days=1)) - today).total_seconds()

    def test_from_today_hour(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            hours=1
        )
        assert calc_seconds() == ((today + dt.timedelta(hours=1)) - today).total_seconds()

    def test_from_today_minutes(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            minutes=1
        )
        assert calc_seconds() == ((today + dt.timedelta(minutes=1)) - today).total_seconds()

    def test_from_today_seconds(self):
        today = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        calc_seconds = expire.from_today(
            seconds=1
        )
        assert calc_seconds() == ((today + dt.timedelta(seconds=1)) - today).total_seconds()

