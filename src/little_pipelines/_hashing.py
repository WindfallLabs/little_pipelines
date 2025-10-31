"""Hashing utilities for pipeline cache invalidation."""

import hashlib
import inspect
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._tasks import Task


def hash_file(filepath: Path|BytesIO) -> str:
    """
    Hash a single file's contents.

    Args:
        filepath: Path to file to hash

    Returns:
        SHA256 hash
    """
    hasher = hashlib.sha256()

    try:
        if hasattr(filepath, "read"):
            f = filepath
        else:
            f = open(filepath, 'rb')

        # Read in chunks for memory efficiency with large files
        for chunk in iter(lambda: f.read(65536), b''):
            hasher.update(chunk)

        return hasher.hexdigest()
    except (OSError, IOError) as e:
        # File doesn't exist or can't be read
        # TODO: hash a dir?
        return "HASHERROR"


def hash_files(*files: Path):
    """Hash many files."""
    s = ""
    for i in files:
        s += hash_file(i)
    # Hash the hashes
    return hashlib.sha256(s.encode("UTF8")).hexdigest()


def hash_script(filepath: Path) -> str:
    """
    Hash the entire module file where the task is defined.

    Args:
        task: Task instance to hash

    Returns:
        SHA256 hash of the task's module file
    """

    try:
        return #hash_file(module_file)
    except (OSError, TypeError, AttributeError):
        return "HASHERROR"
