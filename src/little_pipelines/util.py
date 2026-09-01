"""
Utils
"""

import datetime as dt
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter_ns


HOME = Path().home() / ".little_pipelines"
DEFAULT = HOME / "DEFAULT"
DEFAULT_LOG_DIR = DEFAULT / "logs"
DEFAULT_CACHE_FILE = DEFAULT / "DEFAULT_CACHE"
if not DEFAULT.exists():
    DEFAULT.mkdir()


# TODO: deprecate
def time_diff(start: float, end: float) -> str:
    """Calculates the minutes and seconds difference between two timestamps (floats)."""
    ms = (end - start) / 1000
    tot_secs = dt.timedelta(microseconds=ms).total_seconds()
    min = int(tot_secs // 60)
    sec = tot_secs % 60
    t_msg = f"{min}:{sec:.2f}"
    return t_msg


class Timer:
    """A nanosecond-based process timer."""
    def __init__(self, nanoseconds: int = 0):
        self.total_nanoseconds = nanoseconds
        self._start: int | None = None
        self._end: int | None = None
        self._status: str = "Stopped"

    def format_elapsed(self) -> str:
        """Convert total_nanoseconds to a human-readable M:SS.ss string."""
        if self._status == "Running":
            elapsed_ns = perf_counter_ns() - self._start
        else:
            elapsed_ns = self.total_nanoseconds
        
        total_seconds = elapsed_ns / 1_000_000_000
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:05.2f}"

    def start(self):
        """Start the timer."""
        self._start = perf_counter_ns()
        self._status = "Running"
        return self

    def lap(self):  # TODO: just an unfinished idea
        """Return a Timer for the elapsed time since start."""
        if self._status != "Running":
            raise RuntimeError("Timer is not running")
        elapsed_ns = perf_counter_ns() - self._start
        return self.__class__.from_ns(elapsed_ns)

    def stop(self):
        """Stop the timer."""
        if self._status == "Stopped":
            raise AttributeError("Timer is not running")
        self._end = perf_counter_ns()
        self._status = "Stopped"
        if self._start is None:
            raise RuntimeError("Timer never started")
        self.total_nanoseconds = self._end - self._start
        return self

    @classmethod
    def from_ns(cls, nanoseconds: int):
        timer = cls()
        timer.total_nanoseconds = nanoseconds
        return timer

    def __str__(self):
        return self.format_elapsed()

    def __repr__(self):
        return f"<Timer ({self._status}): {self.format_elapsed()}>"



@contextmanager
def process_timer():
    """Context manager that returns elapsed time as a Timer object."""
    timer = Timer().start()
    try:
        yield timer
    finally:
        timer.stop()
