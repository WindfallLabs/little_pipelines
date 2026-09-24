"""
Messaging and logging infrastructure.
_Written by Copilot_

Responsibilities
----------------

This module owns:

    - Console output
    - File logging
    - Message formatting
    - Thread-safe transport
    - Rich integration

This module does NOT own:

    - Pipeline execution
    - Task execution
    - Cache management
    - Dependency management

Usage
-----

    from little_pipelines.messaging import get_logger

    log = get_logger()

    log.task_start("MyTask")
    log.info(task="MyTask", msg="Doing work...")
    log.task_complete("MyTask", "0.42s")

The logger works whether Tasks are executed through
a Pipeline or manually in a Python shell.
"""

import datetime as dt
import logging
import queue
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Optional, Literal

from rich.console import Console
from rich.logging import RichHandler
from rich.style import Style
from rich.text import Text
from rich.highlighter import NullHighlighter


# ============================================================================
# Options / Config

DEFAULT_TASK_NAME_LEN = 25


class Verbosity(Enum):
    QUIET = logging.WARNING
    NORMAL = logging.INFO
    VERBOSE = logging.DEBUG

    @property
    def level(self) -> int:
        return self.value


# ============================================================================
# Theme Definitions

@dataclass(frozen=True)
class MessageTheme:
    level: str
    task_style: str
    level_style: str
    message_style: str


TASK_START = MessageTheme(
    level="EXEC",
    task_style="blue",
    level_style="blue",
    message_style="blue",
)

PROCESS_START = MessageTheme(
    level="EXEC",
    task_style="bright_black",
    level_style="grey",
    message_style="grey",
)

TASK_COMPLETE = MessageTheme(
    level="DONE",
    task_style="bright_black",
    level_style="green",
    message_style="bright_black",
)

PROCESS_COMPLETE = MessageTheme(
    level="DONE",
    task_style="bright_black",
    level_style="bright_black",
    message_style="bright_black",
)

WARN = MessageTheme(
    level="WARN",
    task_style="bright_black",
    level_style="yellow",
    message_style="yellow",
)

FAIL = MessageTheme(
    level="FAIL",
    task_style="red",
    level_style="red",
    message_style="red",
)

PIPELINE_INFO = MessageTheme(
    level="DONE",
    task_style="bright_black",
    level_style="bright_black",
    message_style="bright_black",
)

PIPELINE_COMPLETE = MessageTheme(
    level="DONE",
    task_style="green",
    level_style="green",
    message_style="bright_white",
)

INFO = MessageTheme(
    level="INFO",
    task_style="bright_black",
    level_style="bright_black",
    message_style="bright_black",
)

SHELL_INFO = MessageTheme(
    level="INFO",
    task_style="bright_black",
    level_style="bright_black",
    message_style="blue",
)

SHELL_COMPLETE = MessageTheme(
    level="OK",
    task_style="bright_black",
    level_style="bright_black",
    message_style="bright_black",
)

SHELL_FAIL = MessageTheme(
    level="FAIL",
    task_style="blue",
    level_style="red bold",
    message_style="red bold",
)


# ============================================================================
# Formatter

class LPFormatter(logging.Formatter):
    """
    Rich formatter for Little Pipelines.

    Expects optional logging record attributes:

        task
        event
        task_style
        level_style
        message_style
    """

    default_time_format = "%Y-%m-%d %H:%M:%S.%f"

    def format(self, record: logging.LogRecord) -> str:
        timestamp: str = (
            dt.datetime.fromtimestamp(record.created)
            .strftime(self.default_time_format)[:-3]
        )

        task = getattr(record, "task", "")
        event = getattr(record, "event", record.levelname)

        task_style = getattr(
            record,
            "task_style",
            "bright_black",
        )

        level_style = getattr(
            record,
            "level_style",
            "bright_black",
        )

        message_style = getattr(
            record,
            "message_style",
            "bright_black",
        )

        task_width = getattr(
            record,
            "task_width",
            DEFAULT_TASK_NAME_LEN,
        )

        level_colors = {"WARNING": "yellow", "ERROR": "red"}
        level_part = f"[{level_colors.get(record.levelname, 'bright_black')}]{record.levelname:<8}[/]"
        time_part = (
            f"[bright_black][{timestamp}][/]"
        )
        task_part = f"  [{task_style}]{task.ljust(task_width)}[/]"
        status_level = f"[{level_style}] :{event.center(6)}:[/]"
        msg_part = f"[{message_style}] {record.getMessage()}[/]"

        return (
            level_part
            + time_part  # NOTE: requires NullHighlighter
            + task_part
            + status_level
            + msg_part
        )


# ============================================================================
# Logger Wrapper

class LPLogger:
    """
    Little Pipelines logger.
    """

    def __init__(
        self,
        name: str = "little_pipelines",
        task_name_width: int = DEFAULT_TASK_NAME_LEN
    ):
        self.name = name
        self._task_name_len = task_name_width
        self.enabled = True
        self.console = Console()
        self._started = False
        self._queue: queue.Queue = queue.Queue()
        self._listener: Optional[QueueListener] = None
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self._verbosity = Verbosity.NORMAL

    # ========================================================================
    # Config

    def set_max_task_name_len(self, width: int = 25) -> None:
        """
        Set the maximum Task name spacing.
        """
        self._task_name_len = width
        return

    def set_verbosity(self, verbosity: Literal["quiet", "normal", "verbose"] = "normal") -> None:
        """
        Set the verbosity of the logger.

        This works by setting the logging level vis-a-vi the Verbosity enum.
        """
        verbosity = verbosity.upper()

        if verbosity == "QUIET":
            self._verbosity = Verbosity.QUIET
            self.console.quiet = True
        else:
            self._verbosity = Verbosity.NORMAL
            self.console.quiet = False
        self._logger.setLevel(Verbosity[verbosity].level)
        return

    @property
    def quiet(self) -> bool:
        return self._verbosity == Verbosity.QUIET
    
    @quiet.setter
    def quiet(self, do_quiet: bool):
        if do_quiet is True:
            self.set_verbosity("quiet")
        else:
            self.set_verbosity("normal")
        return



    # ========================================================================
    # Setup

    def start(self, log_file: Optional[str | Path] = None) -> None:
        if self._started:
            return

        self._logger.handlers.clear()
        queue_handler = QueueHandler(self._queue)
        self._logger.addHandler(queue_handler)

        # Rich output
        rich_handler = RichHandler(
            markup=True,
            show_path=False,
            show_time=False,
            rich_tracebacks=True,
            show_level=False,  # Controls the leading log-level
            highlighter=NullHighlighter(),  # NOTE: critical for forcing Rich to honor our styling
        )
        rich_handler.setFormatter(LPFormatter())
        handlers = [rich_handler]

        # Optional file logging
        if log_file:
            file_handler = logging.FileHandler(
                log_file,
                encoding="utf-8",
            )
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(levelname)s | %(message)s"
                )
            )
            handlers.append(file_handler)

        self._listener = QueueListener(
            self._queue,
            *handlers,
            respect_handler_level=True,
        )
        self._listener.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return

        if self._listener:
            self._listener.stop()
        self._started = False

    # ========================================================================
    # Internal

    def _ensure_started(self):
        if not self._started:
            self.start()

    def _emit(
        self,
        level: int,
        msg: str,
        task: str = "",
        theme: MessageTheme = INFO,
    ):
        if self.enabled is False:
            return

        self._ensure_started()
        self._logger.log(
            level,
            msg,
            extra={
                "task": task,
                "event": theme.level,
                "task_style": theme.task_style,
                "level_style": theme.level_style,
                "message_style": theme.message_style,
                "task_width": self._task_name_len,
            },
        )

    # ========================================================================
    # General Logging

    def info(self, msg: str, task: str = "", ):
        self._emit(
            logging.INFO,
            msg,
            task,
            INFO,
        )

    def warn(self, msg: str, task: str = "", ):
        self._emit(
            logging.WARNING,
            msg,
            task,
            WARN,
        )

    def error(self, msg: str, task: str = "", ):
        self._emit(
            logging.ERROR,
            msg,
            task,
            FAIL,
        )

    @contextmanager
    def spinner(self, status_msg: str):
        with self.console.status(status_msg):
            yield

    # ========================================================================
    # Task Helpers

    def task_start(self, task: str, msg: str | None = None, ):
        self._emit(
            logging.INFO,
            msg or f"Running {task}...",
            task,
            TASK_START,
        )

    def task_complete(self, task: str, elapsed: str, ):
        self._emit(
            logging.INFO,
            f"(completed in {elapsed})" if elapsed else "complete",
            task,
            TASK_COMPLETE,
        )

    def process_start(self, task: str, process: str, ):
        self._emit(
            logging.INFO,
            f"Running {process}...",
            task,
            PROCESS_START,
        )

    def process_complete(self, task: str, process: str, elapsed: Optional[str] = None, ):
        self._emit(
            logging.INFO,
            f"{process} (completed in {elapsed})" if elapsed else f"{process} complete",
            task,
            PROCESS_COMPLETE,
        )

    def pipeline_info(self, msg: str):
        self._emit(
            logging.INFO,
            msg,
            "Pipeline",
            PIPELINE_INFO,
        )

    def pipeline_complete(self, msg: str):
        self._emit(
            logging.INFO,
            msg,
            "Pipeline",
            PIPELINE_COMPLETE,
        )

    def shell_info(self, msg: str):
        self._emit(
            logging.INFO,
            msg,
            "Shell",
            SHELL_INFO,
        )

    def shell_complete(self, msg: str = "OK"):
        self._emit(
            logging.INFO,
            msg,
            "Shell",
            SHELL_COMPLETE,
        )

    def shell_error(self, msg: str):
        self._emit(
            logging.ERROR,
            msg,
            "Shell",
            SHELL_FAIL,
        )


# ============================================================================
# Singleton Access

_GLOBAL_LOGGER: Optional[LPLogger] = None


def get_logger() -> LPLogger:
    """
    Returns the framework logger.

    Lazily initialized.
    """
    global _GLOBAL_LOGGER

    if _GLOBAL_LOGGER is None:
        _GLOBAL_LOGGER = LPLogger()

    return _GLOBAL_LOGGER


__all__ = ["get_logger", "LPLogger"]
