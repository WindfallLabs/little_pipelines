"""
Little Pipelines exceptions
"""

from ._cache_exc import (
    ResultError,
    ResultExistsError,
    ResultNotFoundError,
)

#from ._data_exc import *  # TODO: make or remove
from ._pipeline_exc import (
    CircularDependencyError,
    DuplicateTaskError,
    MissingDependencyError,
    MissingMainProcessError,
    PipelineError,
    PipelineValidationError,
)
from ._task_exc import (
    DependencyNotDeclaredError,
    DependencyNotFoundError,
    DuplicateResultsError,
    MissingOutputError,
    PipelineNotSetError,
    TaskError,
    TaskNotFoundError,
    TaskOutputValidationError,
    UnexpectedOutputError,
)

__all__ = [
    # Cache
    "ResultError",
    "ResultExistsError",
    "ResultNotFoundError",
    # Pipeline
    "CircularDependencyError",
    "DuplicateTaskError",
    "MissingDependencyError",
    "MissingMainProcessError",
    "PipelineError",
    "PipelineValidationError",
    # Task
    "DependencyNotDeclaredError",
    "DependencyNotFoundError",
    "DuplicateResultsError",
    "MissingOutputError",
    "PipelineNotSetError",
    "TaskError",
    "TaskNotFoundError",
    "TaskOutputValidationError",
    "UnexpectedOutputError",
]
