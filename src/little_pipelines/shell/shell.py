"""
Little Pipelines Shell

# GOALS
- The shell should almost never touch an underscore attribute.
- Be the largest consumer of the Data object


TODO / Wishlist
---------------

Vision
    The shell should evolve into the primary interactive workspace
    for analysts using Little Pipelines, not merely a debugging tool.

Principles
    - Optimize for analyst workflows.
    - Keep commands discoverable and self-documenting.
    - Favor exploration over configuration.
    - Keep subclassing as the primary extension mechanism.
    - Do not introduce plugin architectures unless clearly necessary.

Potential Refactors
    - Split framework commands into dedicated modules:
        shell.commands.execution
        shell.commands.cache
        shell.commands.inspection
        shell.commands.validation

    - Move reusable input parsing into:
        shell.parsers

    - Move reusable Rich output helpers into:
        shell.ui

    - Avoid direct access to underscore-prefixed Task/Pipeline internals
      wherever practical; add public APIs instead.

Potential Features
    - Data/Dataset catalog commands:
        datasets
        dataset <name>
        peek <dataset>

    - Dependency exploration:
        upstream <task>
        downstream <task>
        deps <task>

    - Enhanced cache exploration:
        cache-info
        cache-size
        cache-search

    - Shell autocompletion for:
        tasks
        datasets
        cache entries

    - Rich tables and visualizations for inspection commands.

Messaging / Logging
    - Preserve logger.stop() in postcmd().
      It serves as a synchronization point that prevents queued messages
      from printing after the command prompt returns.

    - Maintain standalone operation:
      Tasks should preserve full logging and UX when run outside a Pipeline.

Long-Term Goal
    Little Pipelines should feel like an Analyst's Toolkit:

        Pipelines      -> execution
        Cache          -> persistence
        Data           -> discovery/documentation
        Shell          -> exploration and operations

    The shell should become the natural place to inspect, execute,
    understand, and troubleshoot analytical workflows.

"""

import os
import re
from cmd import Cmd
from graphlib import TopologicalSorter
from typing import Literal, Optional, TYPE_CHECKING

from rich.console import Console
from rich.markdown import Markdown

from little_pipelines.caching import Cache
from little_pipelines.messaging import get_logger, LPLogger
from little_pipelines.pipeline import Pipeline
from .shell_utils import (
    _handle_dataframe_printing_args,
)


def make_heading(title: str, offset=0, char="#"):
    n = int(round((os.get_terminal_size().columns - (offset + len(title))) / 2 , 0) - 2)
    s = char * n
    return f'{s} {title} {s}'


class Shell(Cmd):
    prompt = "> "
    title = "\nLittle-Pipelines Shell"
    console = Console()
    powered_by = True
    pipeline: Optional["Pipeline"] = None,
    cache: Optional[Cache] = None,
    logger: LPLogger = get_logger()

    def __init__(self, pipeline: Pipeline, cache: Cache):
        super().__init__()
        self.pipeline = pipeline
        self.pipeline._shell = f"{self.__class__.__name__} | '{self.title}'"
        self.cache = cache

    # ========================================================================
    # Exit and aliases

    def do_exit(self, inp: str = "") -> Literal[True]:
        """Exits the shell"""
        # if self.pipeline and hasattr(self.pipeline, "cache"):
        #     self.pipeline.cache._conn.close()
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
            self.logger.shell_complete("Ready")
            self.logger.stop()
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
        self.console.print("[green]Ready.[/]")
        if err:
            self.console.print(f"[red]An error in `startup` occured: {err}[/]")
        return

    def _default_shutdown(self, err: Optional[str]=None):
        """Default shell-close behavior."""
        self.logger.shell_complete("Shell closed")
        if err:
            self.console.print(f"[red]An error in `shutdown` occured: {err}[/]")
        
        self.logger.stop()  # flushes messages
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
            self.logger.shell_error(f"{err}")
        return

    # ========================================================================
    # Config

    def do_quiet(self, inp):
        """Greatly reduces message output. Sets logging level to ERROR."""
        self.do_log("ERROR")
        return

    # ========================================================================
    # Reloading
    
    def do_reload(self, inp: str):
        """Reload tasks."""
        self.pipeline.reload_task()
        return

    # ========================================================================
    # Inspection

    def do_tasks(self, inp: str = ""):  # TODO: return a dataframe that includes whether it's got cached data
        """
        Lists all Tasks in the Pipeline.
        
        --sorted - Sorts tasks alphabetically
        """
        self.logger.shell_info("Listing registered tasks...")
        with self.console.status("Loading..."):
            task_list: list[tuple[str, bool]] = self.pipeline.list_tasks(True)
        if "--sort" in inp:
            task_list.sort(key=lambda x: x[0])
        # TODO: check expiry or value is None
        c = len(task_list)
        for tname, has_cache, last_update, reason in task_list:
            if has_cache:
                self.console.print(
                    f"- {tname} ([green]cached[/], [bright_black]{last_update}[/])"
                )
            else:
                self.console.print(
                    # TODO: not an update date, but a warning message
                    f"- {tname} ([yellow]{reason}[/])"
                )
        self.console.print(f"Registered Tasks: [bright_black]{c}[/]")
        return

    def do_has_dependency(self, inp: str):
        """
        Get tasks with the given dependency (task name).
        """
        do_sort: bool = "--sort" in inp
        dep_name = inp.strip().split(" ")[0]
        task_list: list[str] = []
        for task_name in self.pipeline.list_tasks():
            task = self.pipeline.get_task(task_name)
            if dep_name in task._dependency_names:
                task_list.append(task.name)
        if do_sort:
            task_list.sort()
        for tname in task_list:
            self.console.print(tname)
        return

    def do_status(self, inp):  # TODO: WIP
        """Inspect the status of Data."""
        for dataset in Data.all():
            status = dataset.status()
            self.console.print(
                f"{dataset.name:<30}"
                f"{status.state}"
            )
        return

    # -------------
    # TODO: make commands for
    # - datasets
    # - results
    # - dependencies <TaskName>
    # - downstream <TaskName>
    # - upstream <TaskName>
    # -------------

    def do_peek(self, inp: str):  # TODO: add a --details flag
        """
        Preview cached data.
        Optionally set row and column count with:
            --rows=10
            --columns=10
        """
        task_name = inp.split()[0]
        try:
            result = self.cache.get(task_name).data
        except KeyError as e:
            self.logger.shell_fail(e)
            return

        reset_dataframe_printing = _handle_dataframe_printing_args(inp, result)

        # Print it
        self.console.print(result)

        # Reset dataframe cols/rows printing
        if reset_dataframe_printing:
            reset_dataframe_printing()
        return

    def do_info(self, inp: str):
        """
        Print the documentation of the given Task.
        
        Args:
            task_name: str
            --no-markdown: Flag to print docstring as plaintext
            --docstring: Flag to only print the script's docstring
            --funcs: Flag to only print the Task's process documentation

        """
        inp = inp.strip()
        try:
            task_name = inp.split()[0]
            task = self.pipeline.get_task(task_name)
        except IndexError:
            self.logger.shell_fail("`info` requires a task name")
            return
        except KeyError:
            self.logger.shell_fail(f"No such task '{task_name}'")
            return

        docstring, autodoc = task.get_info()
        # Print script docstring
        self.console.print()  # Empty line
        if "--funcs" not in inp:
            if "--no-markdown" not in inp:
                self.console.print(Markdown(docstring))
            else:
                self.console.print(docstring.strip())
            self.console.print("\n")  # Two empty lines

        if "--docstring" not in inp:
            self.console.print(Markdown("__Auto-documented function docs__"))
            self.console.print()  # Empty line
            self.console.print(autodoc)
            self.console.print()  # Empty line
        # End with the path of the task definition
        self.console.print(f"[bright_black]{task._script_path}[/]")
        return

    # ========================================================================
    # Inspection - Cache utils

    def _list_cache(self, inp: str):  # TODO: refactor and consider 'tasks'
        clist = []
        if inp == "--all":
            cached_names = self.cache.keys()
        else:
            cached_names = [i for i in self.cache.keys() if not i.endswith("_hashes")]
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
    def do_clear_cache(self, inp: str):
        """Clear specified Task results from cache, or all data using '.' or '. --all'.

        Args:
            task_name: The Task to clear cached data
            --hard: Clears all cached data, even those set to `expire.never`
        """
        # TODO: raise KeyError if not exists
        task_name = inp.split()[0]
        if not task_name:
            # No input error
            self.logger.shell_fail("Input required: enter a task name, or use '.'")
            return

        ncache = len(self.cache.keys())

        if task_name.startswith("."):  # TODO: BUG: the console.status is wonk
            #if "--hard" in inp:
            self.logger.shell_info("Clearing all cached data...")
            with self.console.status("Clearing all cached data..."):
                self.cache.clear()
            self.logger.shell_info(f"Cleared {ncache} of {ncache} cached results")
            return
        # TODO: add some sort of keep flag?
        else:
            self.logger.shell_info(f"Clearing cached data for {task_name}...")
            self.cache.clear(task_name)
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

    # TODO: support executing one or more tasks: `execute One Two`
    def _execute(self, inp: str) -> None:
        """
        Execute each Task in the Pipeline.
        If a single task is specified, upstream and downstream tasks may also be executed.

        Args:
            --force: Deletes the cache before executing the pipeline
            --skip: Sets a layer to be skipped (can be used multiple times)
            --no-upstream: Does not execute upstream tasks. Only used when one task is specified.
            --no-downstream: Does not execute downstream tasks. Only used when one task is specified.
        """
        inputs: list[str] = inp.split()
        if "--ignore" in inputs:
            raise NameError("No --ignore flag. Did you mean --skip=<task> ?")
        force: bool = ("--force" in inputs)
        upstream: bool = not ("--no-upstream" in inputs)  # Default True
        downstream: bool = not ("--no-downstream" in inputs)  # Default True
        quiet: bool = ("--quiet" in inputs or "-q" in inputs)  # Default False
        target_task_name = inputs[0]
        kwargs = self._clean_kwargs(inputs)
        # Process all tasks
        if target_task_name == ".":
            skipped_tasks: list[str] = self._get_skipped(inputs)
            self.pipeline.execute(force_all=force, skip_tasks=skipped_tasks, quiet=quiet)  # TODO: kwargs?
            return

        self.pipeline.execute_one(target_task_name, force=force, upstream=upstream, downstream=downstream, quiet=quiet, **kwargs)
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
            self.logger.error(f"Error: {e}")
        return

    #@app_logger.catch
    def do_validate(self, inp) -> None:
        """Validates tasks."""  # TODO: more documentation -- what's this do?
        self.logger.shell_info("Validating...")
        self.pipeline.validate_tasks()
        return


__all__ = ["Shell"]
