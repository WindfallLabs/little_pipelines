"""Tests for utility functions."""

from time import perf_counter_ns

from little_pipelines import util


class TestTimeDiff:
    """Test time_diff utility function."""

    def test_time_diff_basic(self):
        """Test basic time difference calculation."""
        start = perf_counter_ns()
        end = start + 1_500_000_000  # 1.5 seconds in nanoseconds

        result = util.time_diff(start, end)

        # Should be in format "M:SS.ss"
        assert isinstance(result, str)
        assert ":" in result

    def test_time_diff_less_than_minute(self):
        """Test time diff for duration less than a minute."""
        start = perf_counter_ns()
        end = start + 30_000_000_000  # 30 seconds in nanoseconds

        result = util.time_diff(start, end)

        # Should show 0 minutes
        assert result.startswith("0:")

    def test_time_diff_more_than_minute(self):
        """Test time diff for duration more than a minute."""
        start = perf_counter_ns()
        end = start + 90_000_000_000  # 90 seconds (1 min 30 sec) in nanoseconds

        result = util.time_diff(start, end)

        # Should show 1 minute
        assert result.startswith("1:")

    def test_time_diff_format(self):
        """Test time diff output format."""
        start = perf_counter_ns()
        end = start + 65_000_000_000  # 65 seconds in nanoseconds

        result = util.time_diff(start, end)

        # Should be in format "M:SS.ss"
        parts = result.split(":")
        assert len(parts) == 2

        minutes = int(parts[0])
        seconds = float(parts[1])

        assert minutes == 1
        assert 4.0 <= seconds <= 6.0  # Should be ~5 seconds

    def test_time_diff_zero_duration(self):
        """Test time diff with zero duration."""
        start = perf_counter_ns()
        end = start

        result = util.time_diff(start, end)

        assert result == "0:0.00"

    def test_time_diff_milliseconds(self):
        """Test time diff with millisecond precision."""
        start = perf_counter_ns()
        end = start + 1_234_000_000  # 1.234 seconds in nanoseconds

        result = util.time_diff(start, end)

        # Should have two decimal places
        assert "." in result
        decimal_part = result.split(".")[1]
        assert len(decimal_part) == 2

    def test_time_diff_large_duration(self):
        """Test time diff with large duration (multiple minutes)."""
        start = perf_counter_ns()
        end = start + 305_000_000_000  # 305 seconds (5 min 5 sec) in nanoseconds

        result = util.time_diff(start, end)

        parts = result.split(":")
        minutes = int(parts[0])
        seconds = float(parts[1])

        assert minutes == 5
        assert 4.0 <= seconds <= 6.0

    def test_time_diff_actual_timing(self):
        """Test time diff with actual sleep timing."""
        import time

        start = perf_counter_ns()
        time.sleep(0.01)  # Sleep for 10ms
        end = perf_counter_ns()

        result = util.time_diff(start, end)

        # Should be close to 0.01 seconds
        parts = result.split(":")
        seconds = float(parts[1])

        # Allow some margin for timing precision
        assert 0.005 <= seconds <= 0.020
