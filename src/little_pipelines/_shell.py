"""Shell"""
import os
from cmd import Cmd
from typing import Literal, Optional

from rich.console import Console

from ._logger import app_logger, reset_app_logger
# from ._execution import (
#     execute_task,
#     execution_order,
#     validate_tasks
# )


def make_heading(title: str, offset=0, char="#"):
    n = int(round((os.get_terminal_size().columns - (offset + len(title))) / 2 , 0) - 2)
    s = char * n
    return f'{s} {title} {s}'


class Shell(Cmd):
    prompt = "> "
    title = "\nLittle-Pipelines Shell"
    console = Console()

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
            app_logger.success("Done.")
        return stop

    def preloop(self):
        app_logger.log("APP", "Shell opened")
        self.console.print()
        self.console.rule(f"[bright_black]{self.title}[/]", style="yellow on black")
        if hasattr(self, "header"):
            self.console.print(self.header)
        self.console.print("[bright_black]powered by Little-Pipelines[/]")
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
        for task in execution_order(False):
            self.console.print(f"- {task.name}")
        return

    #def peek(self, inp):
    #    ...

    # ========================================================================
    # Execution

    @app_logger.catch
    def do_execute(self, inp):
        """Executes a task by name, or use '.' for all tasks."""
        if not inp or inp == ".":
            app_logger.log("APP", "Executing all tasks...")

            for task in execution_order():
                self.console.rule(task.name, style="cyan")
                task.run()

        else:
            self.console.rule(inp, style="cyan")
            app_logger.log("APP", f"===== Executing {inp} =====")
            execute_task(inp)
        return
    
    @app_logger.catch
    def do_validate(self, inp):
        """Validates tasks."""
        app_logger.log("APP", "Validating...")
        validate_tasks()
        return

    @app_logger.catch
    def do_flush(self, inp):
        """Flush all data from Task objects."""
        app_logger.log("APP", "Flushing Task data...")
        ...  # TODO:


__all__ = ["Shell"]


def main() -> None:
    """Main shell loop."""
    Shell().cmdloop()
    return


if __name__ == "__main__":
    main()
