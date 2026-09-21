"""
Policies

Policies determine the status of a dataset, evaluate state; they never mutate framework objects.

Policies never:

    - execute Tasks
    - clear caches
    - mutate data

A Policy simply evaluates state and returns a Status.

The shell and Pipelines may consume those Status
objects however they choose.
"""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from ._hashing import hash_file, hash_files


# ============================================================================
# Status
# ============================================================================

class StatusState(StrEnum):
    CURRENT = "CURRENT"
    CHANGED = "CHANGED"
    UNKNOWN = "UNKNOWN"

    # Future
    REFRESH_DUE = "REFRESH_DUE"
    MISSING_DATA = "MISSING_DATA"
    INVALID = "INVALID"


@dataclass(slots=True)
class Status:
    """
    Policy evaluation result.
    """

    state: StatusState

    reason: str = ""

    details: dict[str, Any] | None = None

    @property
    def is_current(self) -> bool:
        return self.state == StatusState.CURRENT

    @classmethod
    def current(cls, reason: str = "") -> "Status":
        return cls(
            StatusState.CURRENT,
            reason,
        )

    @classmethod
    def changed(cls, reason: str = "") -> "Status":
        return cls(
            StatusState.CHANGED,
            reason,
        )

    @classmethod
    def unknown(cls, reason: str = "") -> "Status":
        return cls(
            StatusState.UNKNOWN,
            reason,
        )


# ============================================================================
# Base Policy
# ============================================================================

class Policy(ABC):
    """
    Base Policy class.
    """

    @abstractmethod
    def check(self) -> Status:
        raise NotImplementedError


# ============================================================================
# Hash Policy
# ============================================================================

class HashPolicy(Policy):
    """
    Determines whether source files have changed.

    Notes
    -----
    This Policy does not persist anything.

    The caller is responsible for remembering the
    previous hash and providing it on construction.
    """

    def __init__(
        self,
        *,
        files: list[str | Path] | None = None,
        script_path: str | Path | None = None,
        previous_hash: str | None = None,
    ):
        self.files = [
            str(f)
            for f in (files or [])
        ]

        self.script_path = (
            str(script_path)
            if script_path
            else None
        )

        self.previous_hash = previous_hash

    @property
    def current_hash(self) -> str:
        parts: list[str] = []

        if self.files:
            parts.append(hash_files(self.files))

        if self.script_path:
            parts.append(hash_file(self.script_path))

        digest = hashlib.sha256()

        for part in parts:
            digest.update(part.encode("utf-8"))

        return digest.hexdigest()

    def check(self) -> Status:
        if self.previous_hash is None:
            return Status.unknown("No previous hash available")

        if self.current_hash != self.previous_hash:
            return Status.changed("Hashes differ")

        return Status.current("Hashes unchanged")


# ============================================================================
# Future Policies
# ============================================================================

class ExpiryPolicy(Policy):
    def check(self) -> Status:
        raise NotImplementedError(
            "ExpiryPolicy planned for a future release."
        )


class CalendarCompletenessPolicy(Policy):
    def check(self) -> Status:
        raise NotImplementedError("CalendarCompletenessPolicy planned for a future release.")


class MultiPolicy(Policy):
    def __init__(self, *policies: Policy):
        self.policies = policies

    def check(self) -> Status:
        raise NotImplementedError("MultiPolicy planned for a future release.")
