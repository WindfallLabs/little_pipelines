# tests/test_shell_inspection.py

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from little_pipelines.shell import Shell


# ============================================================================
# Tasks
# ============================================================================


def test_tasks_reports_totals(shell, mock_pipeline):
    mock_pipeline.list_tasks.return_value = [
        ("task_a", 3),
        ("task_b", 2),
    ]

    shell.do_tasks("")

    mock_pipeline.list_tasks.assert_called_once_with(True)

    printed = str(shell.console.print.call_args_list)

    assert "Total Tasks" in printed
    assert "Total Results" in printed


def test_tasks_sorts_when_requested(
    shell,
    mock_pipeline,
):
    mock_pipeline.list_tasks.return_value = [
        ("z_task", 1),
        ("a_task", 2),
    ]

    shell.do_tasks("--sort")

    calls = shell.console.print.call_args_list

    assert "a_task" in str(calls[0])
    assert "z_task" in str(calls[1])


def test_task_requires_name(shell):
    shell.do_task("")

    shell.logger.shell_fail.assert_called_once_with("Task name required.")


def test_task_displays_metadata(
    shell,
    mock_pipeline,
):
    task = SimpleNamespace(
        name="BuildRoutes",
        dependency_names={"LoadRoads", "LoadParcels"},
        outputs={},
    )

    task.name = "BuildRoutes"
    task.dependency_names = {
        "LoadRoads",
        "LoadParcels",
    }

    mock_pipeline.get_task.return_value = task

    shell.do_task("BuildRoutes")

    printed = str(shell.console.print.call_args_list)

    assert "BuildRoutes" in printed
    assert "Dependencies" in printed
    assert "LoadRoads" in printed
    assert "LoadParcels" in printed


def test_task_displays_outputs(
    shell,
    mock_pipeline,
    task_factory,
):
    task = task_factory(
        name="BuildRoutes",
        outputs={
            "routes": list,
        },
    )

    mock_pipeline.get_task.return_value = task

    shell.do_task("BuildRoutes")

    printed = str(shell.console.print.call_args_list)

    assert "Outputs" in printed
    assert "routes" in printed
    assert "list" in printed


# ============================================================================
# Dependencies
# ============================================================================


def test_upstream_requires_name(shell):
    shell.do_upstream("")

    shell.logger.shell_fail.assert_called_once_with("Task name required.")


def test_upstream_no_dependencies(
    shell,
    mock_pipeline,
):
    mock_pipeline.get_upstream_tasks.return_value = []

    shell.do_upstream("task_a")

    shell.console.print.assert_called_with("[yellow]No upstream tasks.[/]")


def test_upstream_renders_dependencies(
    shell,
    mock_pipeline,
):
    mock_pipeline.get_upstream_tasks.return_value = [
        "dep_a",
        "dep_b",
    ]

    shell.do_upstream("task_a")

    printed = str(shell.console.print.call_args_list)

    assert "Upstream of task_a" in printed
    assert "dep_a" in printed
    assert "dep_b" in printed


def test_downstream_requires_name(shell):
    shell.do_downstream("")

    shell.logger.shell_fail.assert_called_once_with("Task name required.")


def test_downstream_no_dependencies(
    shell,
    mock_pipeline,
):
    mock_pipeline.get_downstream_tasks.return_value = []

    shell.do_downstream("task_a")

    shell.console.print.assert_called_with("[yellow]No downstream tasks.[/]")


def test_downstream_renders_dependencies(
    shell,
    mock_pipeline,
):
    mock_pipeline.get_downstream_tasks.return_value = [
        "dep_a",
        "dep_b",
    ]

    shell.do_downstream("task_a")

    printed = str(shell.console.print.call_args_list)

    assert "Downstream of task_a" in printed
    assert "dep_a" in printed
    assert "dep_b" in printed


def test_has_dependency_lists_matching_tasks(
    shell,
    mock_pipeline,
):
    task_a = Mock(
        name="task_a",
        dependency_names={"source"},
    )
    task_b = Mock(
        name="task_b",
        dependency_names=set(),
    )

    mock_pipeline.list_tasks.return_value = [
        "task_a",
        "task_b",
    ]

    mock_pipeline.get_task.side_effect = [
        task_a,
        task_b,
    ]

    shell.do_has_dependency("source")

    shell.console.print.assert_called_once_with(task_a.name)


# ============================================================================
# Datasets
# ============================================================================


def test_datasets_lists_all(shell):
    dataset = Mock()
    dataset.name = "Parcels"
    dataset.dtype = str

    with patch("little_pipelines.shell.commands.inspection.Data.all") as mock_all:
        mock_all.return_value = [dataset]

        shell.do_datasets()

    printed = str(shell.console.print.call_args_list)

    assert "Parcels" in printed
    assert "Total Datasets" in printed


def test_datasets_sorts(shell, dataset_factory):

    dataset_a = dataset_factory(name="a_dataset", dtype=str)
    dataset_b = dataset_factory(name="b_dataset", dtype=str)

    with patch("little_pipelines.shell.commands.inspection.Data.all") as mock_all:
        mock_all.return_value = [
            dataset_a,
            dataset_b,
        ]

        shell.do_datasets("--sort")

    calls = shell.console.print.call_args_list

    assert "a_dataset" in str(calls[0])
    assert "b_dataset" in str(calls[1])


def test_dataset_requires_name(shell):
    shell.do_dataset("")

    shell.logger.shell_fail.assert_called_once_with("Dataset name required.")


def test_dataset_displays_metadata(shell, dataset_factory):

    dataset = dataset_factory(
        name="Parcels", owner="GIS", source="County", tags=["core"], doc=None
    )

    with patch("little_pipelines.shell.commands.inspection.Data.lookup") as mock_lookup:
        mock_lookup.return_value = dataset

        shell.do_dataset("Parcels")

    printed = str(shell.console.print.call_args_list)

    assert "Parcels" in printed
    assert "GIS" in printed
    assert "County" in printed


def test_status_prints_dataset_states(shell, dataset_factory):
    dataset = dataset_factory(
        name="Parcels",
    )

    status = Mock()
    status.state = "READY"

    dataset.status.return_value = status

    with patch("little_pipelines.shell.commands.inspection.Data.all") as mock_all:
        mock_all.return_value = [dataset]

        shell.do_status()

    printed = str(shell.console.print.call_args_list)

    assert "Parcels" in printed
    assert "READY" in printed


# ============================================================================
# Cached data inspection
# ============================================================================


def test_peek_requires_name(shell):
    shell.do_peek("")

    shell.logger.shell_fail.assert_called_once_with("Task name required.")


def test_peek_missing_cache_entry(
    shell,
    mock_cache,
):
    mock_cache.get.side_effect = KeyError

    shell.do_peek("task_a")

    shell.logger.shell_fail.assert_called_once()


def test_peek_prints_result(
    shell,
    mock_cache,
):
    result = Mock()
    result.data = {"a": 1}

    mock_cache.get.return_value = result

    with patch(
        "little_pipelines.shell.commands.inspection._handle_dataframe_printing_args",
        return_value=None,
        create=True,
    ):
        shell.do_peek("task_a")

    shell.console.print.assert_called_with({"a": 1})
