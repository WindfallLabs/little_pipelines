"""
Cache-related exceptions
"""


class CacheNotSetError(Exception):
    """
    A Pipeline or a Task has not been given a Cache.
    """
    pass


class ResultError(Exception):
    """
    Base class for Result errors.
    """
    pass


class ResultNotFoundError(ResultError):
    """
    A given Result does not exist in the Cache.
    """
    pass


class ResultExistsError(ResultError):
    """
    A given Result does not exist in the Cache.
    """
    pass
