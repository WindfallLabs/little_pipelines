"""
Execution commands for the Little Pipelines shell.
"""

from ..parsers import parse_execute_args


class ExecutionCommands:
    """
    Pipeline execution commands.

    Expected attributes supplied by Shell:

        self.pipeline
        self.logger
        self.console
        self._message_verbosity
    """

    def _configure_execution_logging(self, inp: str) -> None:
        """
        Apply temporary execution verbosity settings.
        """
        args = parse_execute_args(inp)
        if args.quiet:
            self.logger.set_verbosity("quiet")
        if args.verbose:
            self.logger.set_verbosity("verbose")

    def _reset_execution_logging(self) -> None:
        """
        Restore shell verbosity after execution.
        """
        self.logger.set_verbosity(
            self._message_verbosity
        )

    # ======================================================================
    # Core executor

    def _execute(self, inp: str) -> None:
        args = parse_execute_args(inp)
        try:
            target = args.target
            if target == ".":
                self.pipeline.execute(
                    force_all=args.force,
                    skip_tasks=args.skip_tasks,
                    **args.kwargs,
                )
            elif target not in [t.name for t in self.pipeline.tasks]:
                self.logger.shell_error(f"No such task: {target}")
            else:
                self.pipeline.execute_one(
                    target,
                    force=args.force,
                    upstream=args.upstream,
                    downstream=args.downstream,
                    **args.kwargs,
                )
        finally:
            self._reset_execution_logging()

    # ======================================================================
    # Public commands

    def do_execute(self, inp: str) -> None:
        """
        Execute tasks.

        Examples
        --------
            execute .
            execute . --force
            execute BuildRoutes
            execute BuildRoutes --force
            execute BuildRoutes --no-downstream
            execute . --skip=ExportCSV
        """
        self.logger.rule(title="Pipeline", color="green")
        self._execute(inp)
        self.logger.rule(color="green")

    def do_run(self, inp: str) -> None:
        """
        Alias for execute.
        """
        self.do_execute(inp)

    def do_show_traceback(self, inp: str = ""):
        """
        Show the traceback (ExceptionGroup) of the last Pipeline execution.
        """
        self.logger.rule(title="Traceback", color="red")
        if not self.pipeline.execution_errors:
            self.logger.shell_info("No traceback")
            return
        try:
            raise ExceptionGroup("Execution Error(s)", self.pipeline.execution_errors)
        except Exception:
            self.console.print_exception()
        self.logger.rule(color="red")

    def do_trace(self, inp: str = ""):
        """
        Alias for `show-traceback`.
        """
        return self.do_show_traceback()

    def do_reload(self, inp: str) -> None:  # TODO: doesn't work
        """
        Reload pipeline modules.
        """
        if not inp:
            self.pipeline.reload()
            return

        self.pipeline.reload_task(
            inp.strip()
        )
