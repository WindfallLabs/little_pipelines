"""
Task-related exceptions
"""


class TaskError(Exception):
    """
    Base class for Task exceptions.
    """
    pass


class TaskNotFoundError(TaskError):
    """
    Exception raised when a specific Task is not found in the registry.
    """
    pass


class DependencyNotFoundError(TaskError):
    """
    Exception raised when a dependent Result is not in the Cache.
    """
    pass


class DependencyNotDeclaredError(TaskError):
    """
    Exception raised when access to an undeclared dependency occurs.
    """
    pass


class PipelineNotSetError(TaskError):
    """
    Exception raised when a Task has no associated Pipeline and one is required.
    """
    pass


class DuplicateResultsError(TaskError):
    """
    Occurs when a Task returns multiple Results with the same name.
    """
    pass


# ============================================================================
# Task Validation

class TaskOutputValidationError(TaskError):
    """
    Raised when a Task returns an unexpected output type.
    """
    pass


class MissingOutputError(TaskOutputValidationError):
    """
    Raised when an expected output of a Task was not returned.
    """
    pass


class UnexpectedOutputError(TaskOutputValidationError):
    """
    Raised when a Task returns an unexpected output (type).
    """
    pass
