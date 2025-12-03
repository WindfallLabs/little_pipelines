"""Shell"""
import os
from cmd import Cmd
from getpass import getuser
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
    logger = app_logger

    # ========================================================================
    # Setup

    def set_pipeline(self, pipeline: "Pipeline"):
        """Set the pipeline. Call this before `cmdloop`."""
        self.pipeline = pipeline
        self.pipeline._shell = f"{self.__class__.__name__} | '{self.title}' | opened by {getuser()}"
        return self

    # ========================================================================
    # Exit and aliases

    def do_exit(self, inp: str = "") -> Literal[True]:
        """Exits the shell"""
        return True

    def do_quit(self, inp: str = "") -> Literal[True]:
        """Exits the shell"""
        return self.do_exit("")

    def do_q(self, inp: str = "") -> Literal[True]:
        """Exits the shell"""
        return self.do_exit("")

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
            self.logger.success("<light-black>End.</>")
        return stop

    def preloop(self):
        self.console.print()
        self.console.rule(f"[bright_black]{self.title}[/]", style="yellow on black")
        if hasattr(self, "header"):
            self.console.print(self.header)
        if self.powered_by:
            self.console.print("[bright_black]powered by Little-Pipelines[/]")
        self.console.print(f"Loaded pipeline: [bright_blue]{self.pipeline.name}[/]")
        self.logger.log("APP", f"Welcome {getuser()}!")
        self.console.print("[green]Ready.[/]")
        return

    def postloop(self):
        self.logger.log("APP", "Shell closed")
        self.console.rule(style="yellow")
        return

    def precmd(self, line):
         return line

    # ========================================================================
    # Config

    def do_log(self, level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
        """Sets the log level.
        Use:
            `log DEBUG`
        """
        level = level.upper()
        reset_app_logger(level)
        self.console.print(f"Set logging to '{level}'")
        return

    def do_quiet(self, inp):
        """Greatly reduces message output. Sets logging level to ERROR."""
        self.do_log("ERROR")
        return

    # ========================================================================
    # Inspection

    def _list_tasks(self):
        """Formats the string that lists all Tasks in the Pipeline."""
        cached = list(self.pipeline.cache.iterkeys())
        c = 0
        tlist = []
        for task in self.pipeline.tasks:
            tlist.append(
                f"- {task.name} ([green]cached[/])" if task.name in cached else f"- {task.name} ([yellow]not cached[/])"
            )
            c += 1
        tlist.append(f"[bright_black]Total: {c}[/]")
        return tlist

    @app_logger.catch
    def do_tasks(self, inp: str = ""):
        """Lists all Tasks in the Pipeline."""
        self.logger.log("APP", "Listing registered tasks...")
        self.console.print("Registered Tasks:")
        for tmsg in self._list_tasks():
            self.console.print(tmsg)
        return

    def do_peek(self, task_name: str):
        """Preview cached data."""
        r = self.pipeline.get_result(task_name)
        self.console.print(r)
        return
    
    def do_info(self, task_name):
        """Print the docstring of the given Task."""
        task_name = task_name.strip()
        self.console.print(f"[yellow]{self.pipeline.get_task(task_name).info}[/]")
        return

    # ========================================================================
    # Inspection - Cache utils

    def _list_cache(self, inp: str = ""):
        clist = []
        if inp == "--all":
            cached_names = list(self.pipeline.cache.iterkeys())
        else:
            cached_names = [k for k in self.pipeline.cache.iterkeys() if not k.endswith("_hashes")]
        for k in cached_names:
            clist.append(f"- '{k}'")
        clist.append(f"[bright_black]Total: {len(cached_names)}[/]")
        return clist

    def do_cachelist(self, inp):
        """List Task names with cached results."""
        for msg in self._list_cache(inp):
            self.console.print(msg)
        return

    def do_list_cache(self, inp):
        return self.do_cachlist(inp)

    @app_logger.catch
    def do_cacheclear(self, task_name: str):
        """Clear specified Task results from cache, or all data using '.' or '. --all'.
        
        Args:
            task_name: The Task to clear cached data
            --hard: Clears all cached data, even those set to `expire.never`
        """
        task_name = task_name.strip()
        ncache = len([k for k in self.pipeline.cache.iterkeys() if not k.endswith("_hashes")])
        if not task_name:
            # No input error
            self.logger.error("Input required: enter a task name, or use '.'")
            return

        if task_name.startswith("."):
            if "--hard" in task_name:
                self.logger.log("APP", "Clearing all cached data...")
                self.pipeline.cache.clear()
                self.logger.success(f"Cleared {ncache} of {ncache} cached results")
                return

            c = 0
            self.logger.log("APP", "Clearing cached data...")
            for t in self.pipeline.tasks:
                if t.expire_results.__name__ != "expire_never" and t.name in self.pipeline.cache.iterkeys():
                    self.pipeline.cache.delete(t.name)
                    c += 1
            self.logger.success(f"Cleared {c} of {ncache} cached results")
            return
        else:
            self.logger.log("APP", f"Clearing cached data for {task_name}...")
            self.pipeline.cache.delete(task_name)
        return

    def do_clear_cache(self, task_name: str):
        return self.do_cacheclear(task_name)


    # ========================================================================
    # Execution

    def _execute(self, inp: Optional[str] = "") -> None:
        """Execute each Task in the Pipeline."""
        inputs = inp.split()
        skipped = [i.replace("--skip=", "") for i in inputs if i.startswith("--skip=")]
        force = ("--force" in inputs)
        self.pipeline.execute(force=force, skip_tasks=skipped)
        return

    @app_logger.catch
    def do_execute(self, inp):
        """Execute each Task in the Pipeline.
        
        Args:
            --force: Clears cached results, thereby causing all Tasks to execute
            --skip: Flag a Task name to be skipped
        """
        self._execute(inp)
        return

    def _executeone(self, inp: str) -> None:
        """Executes a single Task in the Pipeline."""
        # assume the user knows the risks -- dependencies...
        inp = inp.strip()
        task = self.pipeline.get_task(inp)
        self.logger.warning(f"Executing single task: {inp}")
        result = task.run()
        self.pipeline._cache_result(task, result)
        # TODO: think about providing access to other task processes...
        return

    @app_logger.catch
    def do_executeone(self, inp):
        """Execute a single Task in the Pipeline."""
        self._executeone(inp)
        return

    @app_logger.catch
    def do_validate(self, inp) -> None:
        """Validates tasks."""
        self.logger.log("APP", "Validating...")
        self.pipeline.validate_tasks()
        return


__all__ = ["Shell"]
