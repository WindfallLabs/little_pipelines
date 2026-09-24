"""
Little Pipelines
"""

from . import caching
from . import exc
from . import util
from ._autodoc import _autodoc  # TODO: move to shell utils?
from .caching import Cache, Result
from .data import Data
from .pipeline import Pipeline
from .shell import Shell
from .task import Task, find_tasks
