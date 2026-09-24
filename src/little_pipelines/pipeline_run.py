"""
Metadata object for Pipeline execution.
"""

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PipelineRun:
    """
    Metadata describing a single Pipeline execution.

    Notes
    -----
    This object is intentionally lightweight and
    persistence-friendly.

    Cache is responsible for storing PipelineRuns.
    """
    pipeline_name: str
    start_time: dt.datetime
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    end_time: dt.datetime | None = None
    tasks_total: int = 0
    tasks_executed: int = 0
    tasks_skipped: int = 0
    tasks_failed: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    # ============================================================
    # Lifecycle

    def stop(self) -> None:
        """Mark the run complete."""
        self.end_time = dt.datetime.now()

    # ============================================================
    # Properties

    @property
    def is_completed(self) -> bool:
        return self.end_time is not None

    @property
    def is_succeeded(self) -> bool:
        return self.tasks_failed == 0

    @property
    def duration(self) -> dt.timedelta | None:
        if self.end_time is None:
            return None

        return self.end_time - self.start_time

    @property
    def duration_seconds(self) -> float | None:
        if self.duration is None:
            return None

        return self.duration.total_seconds()

    # ============================================================
    # Persistence Helpers

    def to_record(self) -> dict[str, Any]:
        """
        Convert to a SQLite-friendly record.

        Matches pipeline_runs table columns.
        """
        data = {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time.isoformat(),
            "end_time": (
                self.end_time.isoformat()
                if self.end_time
                else None
            ),
            "tasks_executed": self.tasks_executed,
            "tasks_skipped": self.tasks_skipped,
            "tasks_failed": self.tasks_failed,
            "extra": json.dumps(self.extra),
        }

        return data

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "PipelineRun":
        """
        Reconstruct from a SQLite row.
        """
        run_rec = cls(
            run_id=record["run_id"],
            pipeline_name=record["pipeline_name"],
            start_time=dt.datetime.fromisoformat(
                record["start_time"]
            ),
            end_time=(
                dt.datetime.fromisoformat(
                    record["end_time"]
                )
                if record["end_time"]
                else None
            ),
            tasks_executed=record["tasks_executed"],
            tasks_skipped=record["tasks_skipped"],
            tasks_failed=record["tasks_failed"],
            extra=(
                json.loads(record["extra"])
                if record["extra"]
                else {}
            ),
        )

        return run_rec

    # ============================================================
    # Dunders
    # ============================================================

    def __str__(self):
        if not self.is_completed:
            return f"{self.pipeline_name} (running)"

        return (
            f"{self.pipeline_name} "
            f"({self.tasks_executed} executed, "
            f"{self.tasks_skipped} skipped, "
            f"{self.tasks_failed} failed, "
            f"{self.duration})"
        )

    def __repr__(self):
        return (
            f"<PipelineRun "
            f"{self.pipeline_name!r} "
            f"id={self.run_id[:8]}>"
        )

__all__ = ["PipelineRun"]
