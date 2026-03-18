"""
Cache

A composable cache handler leveraging pickle/shelve.
"""

import datetime as dt
import functools
import pickle
import shelve
from abc import ABC, abstractmethod
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Optional, TYPE_CHECKING

from . import util

if TYPE_CHECKING:
    from ._pipeline import Pipeline


class CachedResult:
    """
    Standardized value returned from the cache.

        value:  The cached data
        cdate:  Datetime of creation/cache
        hash:   SHA256 hash of the cached value
        expiry: A datetime when the value should become None
    """
    def __init__(self, value: Any, cdate: dt.datetime, hash: str, expiry: Optional[dt.datetime]):
        self.value = value
        self.cdate = cdate
        self.hash = hash
        self.expiry: Optional[dt.datetime] = None

    def __bool__(self):
        return self.value is not None

    def is_expired(self, expiry: Optional[dt.datetime] = None):
        """Compares the cached result's expiry date with the current datetime."""
        # An expiry of None is always expired
        #if self.expiry is None:
        #    return True
        if expiry is None:
            if self.expiry is not None:
                expiry = self.expiry
            else:
                expiry = dt.datetime.now()
        return self.cdate <= expiry

    @property
    def keep_cached(self):
        if not self.is_expired() and self.expiry.year == 9999:
            return True
        return False


class CacheBase(ABC):
    @classmethod
    @abstractmethod
    def keys(self) -> list:
        pass

    @classmethod
    @abstractmethod
    def open(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def close(self) -> None:
        pass

    @classmethod
    @abstractmethod
    def contains(self, key: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def get(self, key: str) -> CachedResult:
        pass

    @classmethod
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        pass

    @classmethod
    @abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def clear(self) -> None:
        pass

'''
    def use_cached_results(
        self,
        key: str,
        ignore_expiry=False,
        logical_callback: Optional[Callable] = None,
        log_get_callback: Optional[Callable] = None,
        log_set_callback: Optional[Callable] = None,
    ):
        """
        Function decorator to optionally wrap underlying functions.
        
            key: The identifier for the cached data
            other_logic_callback: Optional logical function (returns True/False) whether to use cached data
            log_get_callback: Optional logging function when cached result is returned
            log_set_callback: Optional logging function when result is cached
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                """Get the cached result if exists."""
                cached_result = self.get(key)
                # Handle another logical check as a callback function
                logic_check = True
                if logical_callback is not None:
                    logic_check = logical_callback()  # True or False
                if cached_result and logic_check:  # and (ignore_expiry and cached_result.is_expired()):
                    if log_get_callback:
                        log_get_callback()
                    # Return cached value
                    return cached_result.value
                # Otherwise, calculate, cache, and return the result of the wrapped func
                result = func(*args, **kwargs)
                self.set(key, result)
                if log_set_callback:
                    log_set_callback()
                return result
            return wrapper
        return decorator
'''

class Cache(CacheBase):
    def __init__(self, cache_dir: Path|str, cache_name: str):
        self.cache_dir: Path = Path(cache_dir)
        self.cache_name: str = cache_name

        # Calculated / set later
        self.db = None
        self.cache_file: Path = Path(cache_dir) / (cache_name + (".db" if not cache_name.endswith(".db") else ""))

    def __enter__(self) -> "Cache":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return False

    def keys(self) -> tuple[str]:
        """List the names (keys) of data in the cache."""
        if self.db is None:
            self.open()
        return tuple([k for k in self.db.keys()])

    def contains(self, key: str) -> bool:
        """Check if the cache contains a named value."""
        self.open()
        return key in self.db

    def open(self) -> None:
        """Open the connection to the cache."""
        if self.db is None:
            self.db = shelve.open(self.cache_file)
        return

    def close(self) -> None:
        """Close the connection to the cache."""
        if self.db is not None:
            self.db.close()
            self.db = None
        return

    def set(self, key: str, value: Any, expiry: Optional[dt.date|dt.datetime] = None) -> None:
        """Write an object to the cache."""
        if expiry is not None and not isinstance(expiry, dt.date):
            raise TypeError("expiry must be date or datetime")
        self.open()
        pkl = pickle.dumps(value)
        cdate = dt.datetime.now()
        _hash = sha256(pkl).hexdigest()
        # Convert date to datetime
        if type(expiry) is dt.date:
            expiry = dt.datetime(expiry.year, expiry.month, expiry.day)
        self.db[key] = (pkl, cdate, _hash, expiry)
        return

    def get(self, key: str, *args, **kwargs) -> CachedResult:
        """
        Read an object from the cache.
        
        Raises KeyError if key doesn't exist.
        """
        self.open()
        if key not in self.db:
            raise KeyError(f"No cached results for task: '{key}'")
        result = self.db.get(key)
        if result is None:
            raise ValueError(f"Cached results is None for task: '{key}'")
        pkl, cdate, _hash, expiry = result
        value = pickle.loads(pkl)
        return CachedResult(value, cdate, _hash, expiry)

    def delete(self, key: str) -> bool:
        """Deletes a value from the cache."""
        self.open()
        if key in self.db:
            del self.db[key]
        return not self.contains(key)

    def clear(self):
        """Deletes all values from the cache."""
        self.open()
        self.db.clear()
        return

    def clear_expired(self):
        """Deletes values that are set to expire."""
        self.open()
        #now = dt.datetime.now()
        for task_name in self.keys():
            r = self.get(task_name)
            if r.is_expired():
                self.delete(task_name)
        return

    def __del__(self):
        self.close()
        return


default_cache = Cache(
    util.DEFAULT_CACHE_FILE.parent,
    util.DEFAULT_CACHE_FILE.name,
)


class DictCache(CacheBase):
    """A cache object that uses a temporary in-memory dict for storage."""
    def __init__(self, *args, **kwargs):
        self.db = dict()

    def keys(self) -> list:
        return [k for k in self.db.keys()]

    def open(self, *args, **kwargs) -> None:
        return None

    def close(self, *args, **kwargs) -> None:
        return None

    def contains(self, key: str, *args, **kwargs) -> None:
        return key in self.db

    def get(self, key: str, *args, **kwargs) -> None:
        return self.db.get(key)

    def set(self, key: str, value: Any, *args, **kwargs) -> None:
        pkl = pickle.dumps(value)
        cdate = dt.datetime.now()
        _hash = sha256(pkl).hexdigest()
        self.db[key] = (value, cdate, _hash)
        return

    def delete(self, name: str, *args, **kwargs) -> None:
        del self.db[name]
        return name not in self.db

    def clear(self, *args, **kwargs) -> None:
        return None

'''
class NullCache(CacheBase):
    """An object that looks and acts like a Cache but does nothing."""
    def __init__(self, *args, **kwargs):
        self.db = None

    def keys(self) -> list:
        return []

    def open(self, *args, **kwargs) -> None:
        return None

    def close(self, *args, **kwargs) -> None:
        return None

    def contains(self, *args, **kwargs) -> None:
        return False

    def get(self, *args, **kwargs) -> CachedResult:
        return NullResult()

    def set(self, *args, **kwargs) -> None:
        return None

    def delete(self, *args, **kwargs) -> bool:
        return True

    def clear(self, *args, **kwargs) -> None:
        return None
'''
