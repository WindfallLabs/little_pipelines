"""
Little Pipelines Shell
"""
import os
import re
from cmd import Cmd
from getpass import getuser
from typing import Literal, Optional, TYPE_CHECKING

from rich.console import Console

from . import _messages as msg, _autodoc

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
    #logger = app_logger

    @property
    def message(self):
        return self.pipeline.message

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
            self.message.write(**msg.SHELL_COMPLETE)
        return stop

    def _default_startup(self, err: Optional[str]=None):
        """Default shell-start behavior."""
        self.console.clear()
        self.console.rule(f"[bright_black]{self.title}[/]", style="yellow on black")
        if hasattr(self, "header"):
            self.console.print(self.header)
        if self.powered_by:
            self.console.print("[bright_black]powered by Little-Pipelines[/]")
        self.console.print(f"Loaded pipeline: [bright_blue]{self.pipeline.name}[/]")
        self.message.write(msg=f"Welcome {getuser()}", **msg.SHELL)
        self.console.print("[green]Ready.[/]")
        if err:
            self.console.print(f"[red]An error in `startup` occured: {err}[/]")
        return

    def _default_shutdown(self, err: Optional[str]=None):
        """Default shell-close behavior."""
        self.message.write(msg="Shell closed", **msg.SHELL_COMPLETE)
        if err:
            self.console.print(f"[red]An error in `shutdown` occured: {err}[/]")
        self.console.rule(style="yellow")
        self.console.print()
        return

    def preloop(self):
        """
        Required for starting the CLI utility.
        Uses the user-defined 'startup' method, if exists.
        """
        if hasattr(self, "startup"):
            try:
                getattr(self, "startup")()
                return
            except Exception as e:
                self._default_startup(err=e)
                return
        else:
            self._default_startup()
        return

    def postloop(self):
        """
        Required for shutting down the CLI utility.
        Uses the user-defined 'shutdown' method, if exists.
        """
        if hasattr(self, "shutdown"):
            try:
                getattr(self, "shutdown")()
                return
            except Exception as e:
                self._default_shutdown(err=e)
                return
        else:
            self._default_shutdown()
        return

    def precmd(self, line: str):
        """Process all initial input"""
        # Tolerance (syntax sugar) for dashes in function calls
        line = re.sub(r"^\S+", lambda x: x.group(0).replace("-", "_"), line)
        # Including when entered after "help"
        line = re.sub(r"^help \S+", lambda x: x.group(0).replace("-", "_"), line)
        return line

    def onecmd(self, line: str):
        try:
            return super().onecmd(line)
        except Exception as e:
            #e.add_note("Error caught by shell")  # TODO: could be useful?
            #err = f"{e.__class__.__name__}: {' '.join(e.args)} ({' '.join(e.__notes__)})"
            err = f"{e.__class__.__name__}: {' '.join(e.args)}"
            self.message.write(msg=f"{err}", **msg.SHELL_FAIL)
        return

    # ========================================================================
    # Config

    # TODO:
    # def do_log(self, level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
    #     """Sets the log level.
    #     Use:
    #         `log DEBUG`
    #     """
    #     level = level.upper()
    #     #reset_app_logger(level)
    #     self.console.print(f"Set logging to '{level}'")
    #     return

    def do_quiet(self, inp):
        """Greatly reduces message output. Sets logging level to ERROR."""
        self.do_log("ERROR")
        return

    # ========================================================================
    # Inspection

    def do_tasks(self, inp: str = ""):
        """Lists all Tasks in the Pipeline."""
        self.message.write(msg="Listing registered tasks...", **msg.SHELL)
        task_list: list[tuple[str, bool]] = self.pipeline.list_tasks(True)
        # TODO: check expiry or value is None
        c = len(task_list)
        for tname, has_cache in task_list:
            self.console.print(
                f"- {tname} ([green]cached[/])" if has_cache else f"- {tname} ([yellow]not cached[/])"
            )
        self.console.print(f"Registered Tasks: [bright_black]{c}[/]")
        return

    def do_peek(self, task_name: str):
        """Preview cached data."""
        try:
            r = self.pipeline.cache.get(task_name).value
            self.console.print(r)
        except KeyError as e:
            self.message.write(msg=e, **msg.SHELL_FAIL)
        return
    
    def do_info(self, inp: str):
        """Print the docstring of the given Task."""
        try:
            task_name = inp.strip()
            task = self.pipeline.get_task(task_name)
        except ValueError:  # Can't split input
            raise 
        self.console.print(f"[yellow]{task.info}[/]")
        self.console.print(f"[bright_black]{task._script_path}[/]")
        return

    # ========================================================================
    # Inspection - Cache utils

    def _list_cache(self, inp: str = ""):
        clist = []
        if inp == "--all":
            cached_names = self.pipeline.cache.keys()
        else:
            cached_names = [i for i in self.pipeline.cache.keys() if not i.endswith("_hashes")]
        for k in cached_names:
            clist.append(f"- '{k}'")
        clist.append(f"[bright_black]Total: {len(cached_names)}[/]")
        return clist

    def do_list_cache(self, inp):
        """List Task names with cached results."""
        for msg in self._list_cache(inp):
            self.console.print(msg)
        return

    #@app_logger.catch
    def do_clear_cache(self, task_name: str):
        """Clear specified Task results from cache, or all data using '.' or '. --all'.
        
        Args:
            task_name: The Task to clear cached data
            --hard: Clears all cached data, even those set to `expire.never`
        """
        # TODO: raise KeyError if not exists
        task_name = task_name.strip()
        ncache = len([k for k in self.pipeline.cache.keys() if not k.endswith("_hashes")])
        if not task_name:
            # No input error
            self.message.write(msg="Input required: enter a task name, or use '.'", **msg.SHELL_FAIL)
            return

        if task_name.startswith("."):  # TODO: BUG: the console.status is wonk
            if "--hard" in task_name:
                self.message.write(msg="Clearing all cached data...", **msg.SHELL)
                with self.message.console.status("Clearing all cached data..."):
                    self.pipeline.cache.clear()
                self.message.write(msg=f"Cleared {ncache} of {ncache} cached results", **msg.SHELL)
                return

            c = 0
            self.message.write(msg="Clearing cached data...", **msg.SHELL)
            for task_name in self.pipeline.cache.keys():
                _result = self.pipeline.cache.get(task_name)
                # Ignore expiries set to "never"
                if not _result.keep_cached:
                    with self.message.console.status(f"Clearing cached result for {task_name}..."):
                        self.pipeline.cache.delete(task_name)
                    c += 1
            self.message.write(msg=f"Cleared {ncache} of {ncache} cached results", **msg.SHELL)
            return
        else:
            self.message.write(msg=f"Clearing cached data for {task_name}...", **msg.SHELL)
            self.pipeline.cache.delete(task_name)
        return


    # ========================================================================
    def do_reload(self, inp: Optional[str] = "") -> None:
        """Reload task definitions from source code (re-import tasks)."""
        self.pipeline.reload_tasks(inp)
        try:
            self.pipeline.cache.delete(inp)
        except:
            pass
        return

    # ========================================================================
    # Execution
    @staticmethod
    def _get_skipped(inputs: list[str]) -> bool:
        """Extract skip instruction from shell input (str)."""
        return [i.replace("--skip=", "") for i in inputs if i.startswith("--skip=")]

    @staticmethod
    def _clean_kwargs(inputs: list[str]) -> dict[str, str]:
        """Extract kwargs from shell input (str)."""
        kwargs = dict()
        for i in inputs:
            if not i.startswith("--") or "=" not in i:
                continue
            k, v = i.removeprefix("--").split("=")  # TODO: requires '=' sep
            kwargs[k] = v
        return kwargs

    # TODO: support executing named tasks: `execute One Two`
    def _execute(self, inp: Optional[str] = "") -> None:
        """Execute each Task in the Pipeline."""
        inputs: list[str] = inp.split()
        force: bool = ("--force" in inputs)
        # Process all tasks
        if inputs[0] == ".":
            skipped_tasks: list[str] = self._get_skipped(inputs)
            self.pipeline.execute(force_all=force, skip_tasks=skipped_tasks)
        # Single task
        else:
            task_name = inputs[0]
            if task_name not in self.pipeline.list_tasks():
                raise KeyError(f"No such task: '{task_name}'")
            kwargs = self._clean_kwargs(inputs)
            if force:
                self.pipeline.cache.delete(task_name)
            self._executeone(task_name, **kwargs)
        return

    #@app_logger.catch
    def do_execute(self, inp):
        """Execute each Task in the Pipeline.
        
        Args:
            --force: Clears cached results, thereby causing all Tasks to execute
            --skip: Flag a Task name to be skipped
        """
        try:
            self._execute(inp)
        except Exception as e:
            self.message.write(msg=f"Error: {e}", **msg.SHELL_FAIL)
        return

    #@app_logger.catch
    def do_validate(self, inp) -> None:
        """Validates tasks."""  # TODO: more documentation -- what's this do?
        self.message.write(msg="Validating...", **msg.SHELL)
        self.pipeline.validate_tasks()
        return


__all__ = ["Shell"]
