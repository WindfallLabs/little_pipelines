"""States"""

from enum import Enum
from typing import Literal


class RawDataOrigin(Enum):
    """Describes the origin of the raw data."""
    INIT = "INIT"
    EXTRACTED = "EXTRACTED"
    FROM_CACHE = "FROM_CACHE"
    DIRECTLY_SET = "DIRECTLY_SET"


class ETLState(Enum):
    INIT = "INIT"
    DOWNLOADED = "DOWNLOADED"
    EXTRACTED = "EXTRACTED"
    TRANSFORMED = "TRANSFORMED"
    VALIDATED = "VALIDATED"
    CACHE_WRITTEN = "CACHE_WRITTEN"
    CACHE_READ = "CACHE_READ"
    LOADED = "LOADED"
    COMPLETE = "COMPLETE"

    def __repr__(self):
        return f"<ETLState.{self.name}>"

    def __lt__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this < oth

    def __le__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this <= oth

    def __gt__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this > oth

    def __ge__(self, other):
        this = self._member_names_.index(self.name)
        oth = other._member_names_.index(other.name)
        return this >= oth


type State = Literal[*ETLState.__members__.keys()]  # type: ignore
