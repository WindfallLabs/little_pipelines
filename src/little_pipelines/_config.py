"""Config"""

from pathlib import Path
from typing import Optional

class _ConfigMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class _Config(metaclass=_ConfigMeta):
    def __init__(self):
        self.log_dir: Optional[Path] = None
        self.cache_dir: Optional[Path] = None

    def __repr__(self):
        return f"<Config()>"

config = _Config()
