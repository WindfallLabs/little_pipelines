"""
Pipeline-related exceptions
"""


class PipelineError(Exception):
    """
    Base class for Pipeline exceptions.
    """
    pass


# ============================================================================
# Pipeline Validation

class PipelineValidationError(PipelineError):
    """
    Base class for Pipeline validation failures.
    """
    pass


class DuplicateTaskError(PipelineValidationError):
    """
    Raised when multiple Tasks share the same name.
    """
    pass


class MissingDependencyError(PipelineValidationError):
    """
    Raised when a declared dependency cannot be resolved.
    """
    pass


class CircularDependencyError(PipelineValidationError):
    """
    Raised when the Task dependency graph contains a cycle.
    """
    pass


class MissingMainProcessError(PipelineValidationError):
    """
    Raised when a Task does not define a 'main' process.
    """
    pass
