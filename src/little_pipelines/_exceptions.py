"""Exceptions"""

class TaskNotFoundError(Exception):
    """Exception raised when a specific Task is not found in the registry."""
    pass


class DependencyFailure(Exception):
    """Exception raised when one or more dependencies of a Task have failed."""
    pass
