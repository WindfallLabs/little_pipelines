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

from __future__ import annotations

import datetime as dt
import logging
import queue
from dataclasses import dataclass
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Optional

from rich.logging import RichHandler

# ============================================================================
# Theme Definitions
# ============================================================================


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

# ============================================================================
# Formatter
# ============================================================================


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

        timestamp = dt.datetime.fromtimestamp(
            record.created
        ).strftime(self.default_time_format)[:-3]

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
            20,
        )

        time_part = (
            f"[bright_black][{timestamp}][/]"
        )

        task_part = (
            f"  [{task_style}]"
            f"{task.ljust(task_width)}"
            f"[/]"
        )

        level_part = (
            f"[{level_style}]"
            f" :{event.center(6)}: "
            f"[/]"
        )

        msg_part = (
            f"[{message_style}]"
            f"{record.getMessage()}"
            f"[/]"
        )

        return (
            time_part
            + task_part
            + level_part
            + msg_part
        )


# ============================================================================
# Logger Wrapper
# ============================================================================


class LPLogger:
    """
    Framework-facing logger wrapper.

    Users should never need to interact with
    Python logging directly.

    Tasks and Pipelines should use this API.
    """

    def __init__(
        self,
        name: str = "little_pipelines",
    ):
        self.name = name

        self._started = False

        self._queue: queue.Queue = queue.Queue()

        self._listener: Optional[
            QueueListener
        ] = None

        self._logger = logging.getLogger(
            self.name
        )

        self._logger.setLevel(
            logging.INFO
        )

        self._logger.propagate = False

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def start(
        self,
        log_file: Optional[str | Path] = None,
    ) -> None:

        if self._started:
            return

        self._logger.handlers.clear()

        queue_handler = QueueHandler(
            self._queue
        )

        self._logger.addHandler(
            queue_handler
        )

        # Rich output

        rich_handler = RichHandler(
            markup=True,
            show_path=False,
            show_time=False,
            rich_tracebacks=True,
        )

        rich_handler.setFormatter(
            LPFormatter()
        )

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

            handlers.append(
                file_handler
            )

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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

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
            },
        )

    # ------------------------------------------------------------------
    # General Logging
    # ------------------------------------------------------------------

    def info(
        self,
        msg: str,
        task: str = "",
    ):

        self._emit(
            logging.INFO,
            msg,
            task,
            INFO,
        )

    def warning(
        self,
        msg: str,
        task: str = "",
    ):

        self._emit(
            logging.WARNING,
            msg,
            task,
            WARN,
        )

    def error(
        self,
        msg: str,
        task: str = "",
    ):

        self._emit(
            logging.ERROR,
            msg,
            task,
            FAIL,
        )

    # ------------------------------------------------------------------
    # Task Helpers
    # ------------------------------------------------------------------

    def task_start(
        self,
        task: str,
        msg: str | None = None,
    ):

        self._emit(
            logging.INFO,
            msg or f"Running {task}...",
            task,
            TASK_START,
        )

    def task_complete(
        self,
        task: str,
        elapsed: str,
    ):

        self._emit(
            logging.INFO,
            f"(completed in {elapsed})",
            task,
            TASK_COMPLETE,
        )

    def process_start(
        self,
        task: str,
        process: str,
    ):

        self._emit(
            logging.INFO,
            f"Running {process}...",
            task,
            PROCESS_START,
        )

    def process_complete(
        self,
        task: str,
        process: str,
        elapsed: str,
    ):

        self._emit(
            logging.INFO,
            f"{process} (completed in {elapsed})",
            task,
            PROCESS_COMPLETE,
        )

    def pipeline_complete(
        self,
        msg: str,
    ):

        self._emit(
            logging.INFO,
            msg,
            "Pipeline",
            PIPELINE_COMPLETE,
        )


# ============================================================================
# Singleton Access
# ============================================================================

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
