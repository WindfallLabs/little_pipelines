"""
Validation commands for the Little Pipelines shell.
"""


class ValidationCommands:
    """
    Validation commands.

    Expected shell attributes:

        self.pipeline
        self.logger
        self.console
    """

    def do_validate(  # TODO: rename 'check' ?
        self,
        inp: str = "",
    ) -> None:
        """
        Validate the pipeline.

        Examples
        --------

            validate

        Runs all pipeline validation checks and raises any
        validation failures encountered.
        """

        self.logger.shell_info(
            "Validating..."
        )

        self.pipeline.validate_tasks()

        self.logger.shell_info(
            "Validation completed."
        )

    def do_validate_task(
        self,
        inp: str,
    ) -> None:
        """
        Validate a specific task.
        """

        task_name = inp.strip()

        if not task_name:
            self.logger.shell_fail(
                "Task name required."
            )
            return

        task = self.pipeline.get_task(
            task_name
        )

        self.console.print(
            f"Task: {task.name}"
        )

        if not task.has_main:
            raise ValueError(
                f"{task.name} is missing a main process."
            )

        self.console.print(
            "[green]Valid.[/]"
        )
