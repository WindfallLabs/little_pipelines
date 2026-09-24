"""
Little Pipelines Shell

The Shell is Little Pipelines' interactive analyst workspace.

Responsibilities
----------------
- Command lifecycle
- Pipeline and Cache references
- Logger integration
- Error handling
- Startup and shutdown hooks

Implementation Notes
--------------------
Actual commands live in:

    shell.commands.execution
    shell.commands.inspection
    shell.commands.cache
    shell.commands.validation

Shell subclasses should add analyst-specific commands by defining
additional do_* methods.
"""

from __future__ import annotations

import re
from cmd import Cmd

from rich.console import Console

from little_pipelines.caching import Cache
from little_pipelines.messaging import LPLogger, get_logger
from little_pipelines.pipeline import Pipeline

from .commands import (
    CacheCommands,
    ExecutionCommands,
    InspectionCommands,
    ValidationCommands,
)


class Shell(
    InspectionCommands,
    CacheCommands,
    ExecutionCommands,
    ValidationCommands,
    Cmd,
):
    """
    Base interactive shell.

    Intended to be subclassed by analysts for project-specific tools.
    """

    prompt = "> "

    title = "\nLittle Pipelines Shell"

    powered_by = True

    console: Console = Console()

    logger: LPLogger = get_logger()

    _message_verbosity = "normal"

    def __init__(
        self,
        pipeline: Pipeline,
        cache: Cache | None = None,
    ):
        super().__init__()

        self.pipeline = pipeline
        self.cache = cache or pipeline.cache

    # ======================================================================
    # Exit aliases

    def do_exit(self, inp: str = ""):
        """
        Exit the shell.
        """
        return True

    def do_quit(self, inp: str = ""):
        """
        Exit the shell.
        """
        return self.do_exit(inp)

    def do_q(self, inp: str = ""):
        """
        Exit the shell.
        """
        return self.do_exit(inp)

    # ======================================================================
    # Cmd hooks

    def emptyline(self):
        return ""

    def precmd(self, line: str):

        # allow:
        #
        #     list-cache
        #
        # as shorthand for:
        #
        #     list_cache

        line = re.sub(
            r"^\S+",
            lambda m: m.group(0).replace("-", "_"),
            line,
        )

        line = re.sub(
            r"^help \S+",
            lambda m: m.group(0).replace("-", "_"),
            line,
        )

        return line

    def onecmd(self, line: str):
        try:
            return super().onecmd(line)

        except Exception as exc:

            error = (
                f"{exc.__class__.__name__}: "
                f"{' '.join(map(str, exc.args))}"
            )

            self.logger.shell_error(error)

    def postcmd(self, stop, line):

        if line.strip() and line not in {
            "exit",
            "quit",
            "q",
        }:
            self.logger.shell_complete("Ready")
            self.logger.stop()

        return stop

    # ======================================================================
    # Startup / Shutdown

    def _default_startup(
        self,
        err: Exception | None = None,
    ):

        self.console.clear()

        self.console.rule(
            f"[bright_black]{self.title}[/]",
            style="yellow on black",
        )

        if hasattr(self, "header"):
            self.console.print(self.header)

        if self.powered_by:
            self.console.print(
                "[bright_black]powered by Little Pipelines[/]"
            )

        self.console.print(
            f"Loaded pipeline: "
            f"[bright_blue]{self.pipeline.name}[/]"
        )

        self.console.print("[green]Ready.[/]")

        if err:
            self.console.print(
                f"[red]Startup error: {err}[/]"
            )

    def _default_shutdown(
        self,
        err: Exception | None = None,
    ):

        self.logger.shell_complete("Shell closed")

        if err:
            self.console.print(
                f"[red]Shutdown error: {err}[/]"
            )

        self.logger.stop()

        self.console.rule(style="yellow")
        self.console.print()

    def preloop(self):

        startup = getattr(self, "startup", None)

        if startup is None:
            return self._default_startup()

        try:
            startup()

        except Exception as exc:
            self._default_startup(exc)

    def postloop(self):

        shutdown = getattr(self, "shutdown", None)

        if shutdown is None:
            return self._default_shutdown()

        try:
            shutdown()

        except Exception as exc:
            self._default_shutdown(exc)

    # ======================================================================
    # Shell configuration

    def do_quiet(self, inp):

        """
        Reduce shell logging output.
        """

        self._message_verbosity = "quiet"

        self.logger.set_verbosity(
            self._message_verbosity
        )


__all__ = ["Shell"]
