"""Logging utilities."""

import logging
import os
from asyncio import proactor_events
from pathlib import Path

from rich.console import Console

proactor_events.logger.setLevel(logging.ERROR)

DEFAULT_FORMAT = "[{asctime}] [{levelname}/{name}/{filename}:{funcName}]: {message}"
CONSOLE_FORMAT = "[bright_black][{asctime}][/] {levelname}:\t{message}###[bright_black]{filename}[/]"

console = Console()

class RichConsoleHandler(logging.Handler):
    """Handler that writes formatted log messages to a rich.console."""

    def __init__(self, formatter: logging.Formatter, level=logging.DEBUG):
        super().__init__(level)
        self.setFormatter(
            logging.Formatter(CONSOLE_FORMAT, style="{")
        )
        self.console = Console()

    def emit(self, record):
        """Emit a record - this is called by the logging system."""
        try:
            msg = self.format(record)
            msg = msg.replace("DEBUG", "[green]DEBUG[/]")
            msg = msg.replace("INFO", "[blue]INFO[/]")
            msg = msg.replace("WARNING", "[yellow]WARNING[/]")
            msg = msg.replace("ERROR", "[red]ERROR[/]")
            msg = msg.replace("CRITICAL", "[bright_red]CRITICAL[/]")
            left, right = msg.split('###')
            #print("[2025-10-07 16:59:10,988] INFO: Something fun!" + " "*61 + "<ipython-input-2-b4eb05557681>")
            #c_width = os.get_terminal_size().columns
            j = self.console.width - len(left)
            jspaces = max(0, self.console.width - (len(left) + len(right)))
            msg = f"{left}{" " * jspaces}{right}"
            self.console.print(msg)
        except Exception:
            self.handleError(record)


def make_logger(
    name: str,
    log_dir: Path|None=None,
    format=None,
    level=logging.DEBUG
) -> logging.Logger:
    # Default format
    if format is None:
        format = DEFAULT_FORMAT
    formatter = logging.Formatter(format, style="{")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Add console handler (create an instance, not the class)
    console_handler = RichConsoleHandler(formatter, level)
    logger.addHandler(console_handler)

    # Add file handler
    if log_dir is not None:
        log_file = log_dir.joinpath(f"{name}.log")
        file_handler = logging.FileHandler(log_file)  # TODO: error handling
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    return logger


__all__ = [
    "make_logger",
    #"IOLogger",
]
