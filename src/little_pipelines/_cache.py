"""Cache"""

import atexit
from pathlib import Path
from typing import Optional

import diskcache

_CACHE = None

class CacheName:
    """Allows for injecting custom cache filenames."""
    def __init__(self, cache_name: str):
        self.original: str = diskcache.core.DBNAME
        self.new = cache_name

    def __enter__(self):
        """Called when entering the context"""
        diskcache.core.DBNAME = self.new
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called when exiting the context"""
        diskcache.core.DBNAME = self.original
        
        # Return False to propagate exceptions, True to suppress them
        return False


def get_cache(name: Optional[str] = None) -> diskcache.Cache:
    """Connects to the cache file (creates if needed)."""
    cache_dir = Path().home() / ".little_pipelines"
    if name:
        cache_dir = cache_dir / name
    if not cache_dir.exists():
        cache_dir.mkdir()
    #if name:
    #    with CacheName(name):
    #        cache = diskcache.Cache(str(cache_dir), tag_index=True)
    #else:
    cache = diskcache.Cache(str(cache_dir), tag_index=True)
    global _CACHE
    _CACHE = cache
    return cache


def get_tags(cache: diskcache.Cache) -> set[str]:
    """Returns a set of tags."""
    return {cache.get(k, tag=True)[1] for k in cache.iterkeys()}


def inspect_cache(cache: diskcache.Cache) -> list[tuple[str, type]]:
    """Return a list of tuples of keys and value types."""
    return [(k, type(cache.get(k))) for k in cache.iterkeys()]


def remove_cache():
    if list(_CACHE.iterkeys()) == []:
        (Path(_CACHE.directory) / "cache.db").rmdir()
    return


atexit.register(remove_cache)
