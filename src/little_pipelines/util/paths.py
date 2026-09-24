"""
Paths
"""

from pathlib import Path

HOME = Path().home() / ".little_pipelines"
if not HOME.exists():
    HOME.mkdir()

DEFAULT_LOG_DIR = HOME / "logs"
DEFAULT_LOG_DIR.mkdir(exist_ok=True)
