"""
Inspection commands for the Little Pipelines shell.
"""

from __future__ import annotations

from rich.markdown import Markdown

from little_pipelines.data import Data

from ..ui import (
    render_dependency_list,
    render_task_list,
    render_task_totals,
)


class InspectionCommands:
    """
    Inspection and exploration commands.

    Expected shell attributes:

        self.pipeline
        self.cache
        self.console
        self.logger
    """

    # ======================================================================
    # Tasks

    def do_tasks(self, inp: str = "") -> None:
        """
        List registered tasks.

        Options
        -------
            --sort
        """

        self.logger.shell_info(
            "Listing registered tasks..."
        )

        with self.console.status("Loading..."):
            tasks = self.pipeline.list_tasks(True)

        if "--sort" in inp:
            tasks.sort(
                key=lambda x: x[0].lower()
            )

        total_tasks = len(tasks)
        total_results = 0
        for _, result_count in tasks:
            # TODO: get actual results count
            total_results += result_count

        render_task_list(
            self.console,
            tasks,
        )

        render_task_totals(
            self.console,
            total_tasks,
            total_results,
        )

        return

    def do_task(self,inp: str) -> None:
        """
        Show task details.

        Example
        -------
            task BuildRoutes
        """
        task_name = inp.strip()

        if not task_name:
            self.logger.shell_fail("Task name required.")
            return

        task = self.pipeline.get_task(task_name)

        self.console.print()
        self.console.print(f"[b]Name:[/] {task.name}")
        self.console.print(
            f"[b]Dependencies:[/] "
            f"{len(task.dependency_names)}"
        )

        if task.dependency_names:
            for dep in sorted(task.dependency_names):
                self.console.print(f"    - {dep}")

        outputs = getattr(task, "outputs", {},)
        if outputs:
            self.console.print("\n[b]Outputs:[/]")
            for name, dtype in outputs.items():
                dtype_name = getattr( dtype, "__name__", str(dtype))
                self.console.print(
                    f"    - {name} "
                    f"({dtype_name})"
                )

        return

    # ======================================================================
    # Dependencies

    def do_upstream(self, inp: str) -> None:
        """
        Show all upstream tasks for a task.

        Examples
        --------

            upstream BuildRoutes

        Returns all dependencies required to execute the task,
        in execution order.
        """

        task_name = inp.strip()

        if not task_name:
            self.logger.shell_fail(
                "Task name required."
            )
            return

        tasks = self.pipeline.get_upstream_tasks(task_name)
        if not tasks:
            self.console.print(
                "[yellow]No upstream tasks.[/]"
            )
            return

        render_dependency_list(
            self.console,
            f"Upstream of {task_name}",
            tasks,
        )

        render_dependency_list(
            self.console,
            f"Downstream of {task_name}",
            tasks,
        )

    def do_downstream(self, inp: str) -> None:
        """
        Show all downstream tasks for a task.

        Examples
        --------

            downstream BuildRoutes

        Returns every task that depends on this task,
        directly or indirectly.
        """

        task_name = inp.strip()
        if not task_name:
            self.logger.shell_fail("Task name required.")
            return

        tasks = self.pipeline.get_downstream_tasks(task_name)
        if not tasks:
            self.console.print("[yellow]No downstream tasks.[/]")
            return

        self.console.print(f"[b]Downstream of {task_name}[/]")
        for task in tasks:
            self.console.print(f"  ├─ {task}")

        return

    def do_has_dependency(self,inp: str) -> None:
        """
        List tasks having the specified dependency.
        """

        dependency_name = (
            inp.strip()
            .split(maxsplit=1)[0]
        )

        if not dependency_name:
            self.logger.shell_fail("Dependency name required.")
            return

        matching = []
        for task_name in self.pipeline.list_tasks():
            task = self.pipeline.get_task(
                task_name
            )
            if dependency_name in task.dependency_names:
                matching.append(task.name)

        if "--sort" in inp:
            matching.sort()

        for task_name in matching:
            self.console.print(task_name)

        return

    # ======================================================================
    # Data inspection

    def do_datasets(self, inp: str = "") -> None:
        """
        List registered datasets.
        """

        datasets = Data.all()

        if "--sort" in inp:

            datasets = sorted(
                datasets,
                key=lambda d: d.name.lower(),
            )

        for d in datasets:
            dtype = (
                d.dtype.__name__
                if d.dtype
                else "Any"
            )
            self.console.print(f"- {d.name} ([blue]{dtype}[/])")

        self.console.print(f"\nTotal Datasets: [blue]{len(datasets)}[/]")

        return

    def do_dataset(self, inp: str) -> None:
        """
        Show dataset metadata.

        Example
        -------
            dataset Parcels
        """

        dataset_name = inp.strip()
        if not dataset_name:
            self.logger.shell_fail("Dataset name required.")
            return

        dataset = Data.lookup(
            dataset_name
        )

        dtype = (
            dataset.dtype.__name__ if dataset.dtype else "Any"
        )

        self.console.print(f"[b]Name:[/] {dataset.name}")
        self.console.print(f"[b]Type:[/] {dtype}")

        if dataset.owner:
            self.console.print(f"[b]Owner:[/] {dataset.owner}")

        if dataset.source:
            self.console.print(f"[b]Source:[/] {dataset.source}")

        if dataset.tags:
            self.console.print(f"[b]Tags:[/] {', '.join(dataset.tags)}")

        if dataset.doc:
            self.console.print("\n")
            self.console.print(Markdown(dataset.doc))
        
        return

    def do_status(self, inp: str = "") -> None:
        """
        Show dataset status.
        """

        for dataset in Data.all():
            status = dataset.status()
            self.console.print(f"{dataset.name:<30}{status.state}")

        return

    # ======================================================================
    # Cached data inspection

    def do_peek(self, inp: str) -> None:
        """
        Preview cached data.

        Examples
        --------

            peek BuildRoutes

            peek BuildRoutes --rows=20
        """

        if not inp:
            self.logger.shell_fail("Task name required.")

            return

        task_name = inp.split()[0]
        try:
            result = self.cache.get(task_name).data
        except KeyError:
            self.logger.shell_fail(f"No cached result for '{task_name}'")

            return

        #
        # existing helper
        #

        from ..shell_utils import (
            _handle_dataframe_printing_args,
        )

        reset = _handle_dataframe_printing_args(inp, result)

        self.console.print(result)

        if reset:
            reset()

        return
