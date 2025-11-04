"""Shell"""
import os
from cmd import Cmd
from typing import Literal, Optional, TYPE_CHECKING

from rich.console import Console

from ._logger import app_logger, reset_app_logger

if TYPE_CHECKING:
    from ._pipeline import Pipeline


def make_heading(title: str, offset=0, char="#"):
    n = int(round((os.get_terminal_size().columns - (offset + len(title))) / 2 , 0) - 2)
    s = char * n
    return f'{s} {title} {s}'


class Shell(Cmd):
    prompt = "> "
    title = "\nLittle-Pipelines Shell"
    console = Console()
    powered_by = True
    pipeline: Optional["Pipeline"] = None

    # ========================================================================
    # Setup

    def set_pipeline(self, pipeline: "Pipeline"):
        """Set the pipeline. Call this before `cmdloop`."""
        self.pipeline = pipeline
        return self

    # ========================================================================
    # Exit and aliases

    def do_exit(self, inp: Optional[str]):
        """Exits the shell"""
        return True

    def do_quit(self, inp):
        """Exits the shell"""
        return self.do_exit(inp)

    def do_q(self, inp):
        """Exits the shell"""
        return self.do_exit(inp)

    # ========================================================================
    # Hooks

    def emptyline(self):
        return ""

    def postcmd(self, stop, line):
        if line in ("exit", "quit", "q"):
            return stop
        elif str(line).strip(" ") == "":
            return stop
        else:
            # Use 'End' as it doesn't indicate successful execution
            app_logger.success("<light-black>End.</>")
        return stop

    def preloop(self):
        app_logger.log("APP", "Shell opened")
        self.console.print()
        self.console.rule(f"[bright_black]{self.title}[/]", style="yellow on black")
        if hasattr(self, "header"):
            self.console.print(self.header)
        if self.powered_by:
            self.console.print("[bright_black]powered by Little-Pipelines[/]")
        self.console.print(f"Loaded pipeline: [bright_blue]{self.pipeline.name}[/]")
        self.console.print("[green]Ready.[/]")
        return

    def postloop(self):
        app_logger.log("APP", "Shell closed")
        self.console.rule(style="yellow")
        return

    def precmd(self, line):
         return line

    # ========================================================================
    # Config

    def do_log(self, inp: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
        """Sets the log level.
        Use:
            `log DEBUG`
        """
        inp = inp.upper()
        reset_app_logger(inp)
        return

    def do_quiet(self, inp):
        """Greatly reduces message output. Sets logging level to ERROR."""
        self.do_log("ERROR")
        return

    # ========================================================================
    # Inspection

    @app_logger.catch
    def do_tasks(self, inp):
        """Lists all registered tasks (in topological order)."""
        app_logger.log("APP", "Listing registered tasks...")
        self.console.print("Registered Tasks:")
        for task in self.pipeline.tasks(False):
            self.console.print(f"- {task.name}")
        return

    def do_peek(self, inp):
        """Preview cached data."""
        print(self.pipeline.get_result(inp))
        return

    # ========================================================================
    # Execution

    @app_logger.catch
    def do_execute(self, inp):
        """Executes a task by name, or use '.' for all tasks."""
        inputs = inp.split()
        task_name = inputs[0]
        force = ("--force" in inputs)
        if task_name == ".":
            self.pipeline.execute(force=force)

        # Execute a single task -- assume the user knows the risks.
        else:
            task = self.pipeline.get_task(task_name)
            app_logger.warning(f"Executing single task: {task_name}")
            result = task.run()
            self.pipeline._cache_result(task, result)
        return

    @app_logger.catch
    def do_validate(self, inp):
        """Validates tasks."""
        app_logger.log("APP", "Validating...")
        self.pipeline.validate_tasks()
        return

    @app_logger.catch
    def do_clear(self, inp):
        """Clear all data from cache."""
        app_logger.log("APP", "Clearing cached data...")
        self.pipeline.cache.clear()
        return


__all__ = ["Shell"]
