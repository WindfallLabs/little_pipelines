"""
Exceptions
"""

# Work-in-progress: not all these are used or needed


class DependencyNotFoundError(Exception):
    """Exception raised when a dependancy Result is not in the cache."""
    pass


class TaskNotFoundError(Exception):
    """Exception raised when a specific Task is not found in the registry."""
    pass


class PipelineNotSetError(Exception):
    """Exception raised when a Task has no associated Pipeline."""
    pass


class PipelineValidationError(Exception):
    """Base class for Pipeline validation failures."""
    pass


class DuplicateTaskError(PipelineValidationError):
    """Raised when multiple Tasks share the same name."""
    pass


class MissingDependencyError(PipelineValidationError):
    """Raised when a declared dependency cannot be resolved."""
    pass


class CircularDependencyError(PipelineValidationError):
    """Raised when the Task dependency graph contains a cycle."""
    pass


class MissingMainProcessError(PipelineValidationError):
    """Raised when a Task does not define a main process."""
    pass


class OutputValidationError(PipelineValidationError):
    """Raised when a Task returns an unexpected output type."""
    pass


class MissingOutputError(OutputValidationError):
    """Raised when an expected output was not returned."""
    pass


class UnexpectedOutputError(OutputValidationError):
    """Raised when an undeclared output was returned."""
    pass


class ValidationWarning(UserWarning):
    """Non-fatal validation issue."""
    pass
