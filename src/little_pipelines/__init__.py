"""
Little Pipelines
"""
from . import caching
from .caching import Cache, Data, Result  # new Data
from . import exc
from . import expire
from . import util
from ._autodoc import _autodoc
from .pipeline import Pipeline
from .shell import Shell
from .tasks import Task, find_tasks
