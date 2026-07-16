"""
Exceptions
"""

# Work-in-progress: not all these are used or needed


# class DependencyFailure(Exception):
#     """Exception raised when one or more dependencies of a Task have failed."""
#     pass


class DependencyNotFoundError(Exception):
    """Exception raised when a dependancy Result is not in the cache."""
    pass

class TaskNotFoundError(Exception):
    """Exception raised when a specific Task is not found in the registry."""
    pass


class PipelineValidationError(Exception):
    """Exception raised on invalid pipeline."""
    pass


class PipelineNotSetError(Exception):
    """Exception raised when a Task has no associated Pipeline."""
    pass
