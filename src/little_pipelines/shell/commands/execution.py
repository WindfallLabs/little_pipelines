"""
Execution commands for the Little Pipelines shell.
"""

from __future__ import annotations

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

    def _configure_execution_logging(
        self,
        #inputs: list[str],
        inp: str
    ) -> None:
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

    def _execute(
        self,
        inp: str,
    ) -> None:

        args = parse_execute_args(inp)

        try:

            target = args.target

            # ==========================================================
            # Pipeline execution

            if target == ".":

                self.pipeline.execute(
                    force_all=args.force,
                    skip_tasks=args.skip_tasks,
                    **args.kwargs,
                )

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

            self.console.rule(
                style="yellow"
            )

    # ======================================================================
    # Public commands

    def do_execute(
        self,
        inp: str,
    ) -> None:
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

        self._execute(inp)

    def do_run(  # Alias for execute
        self,
        inp: str,
    ) -> None:
        """
        Alias for execute.
        """

        self.do_execute(inp)

    def do_reload(
        self,
        inp: str,
    ) -> None:
        """
        Reload pipeline modules.
        """

        if not inp:

            self.pipeline.reload()

            return

        self.pipeline.reload_task(
            inp.strip()
        )
