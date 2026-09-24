"""
Rich UI helpers for the Little Pipelines shell.

This module should contain presentation logic only.

It should not:
    - Execute commands
    - Parse user input
    - Access private Pipeline APIs
"""

from __future__ import annotations

from rich.markdown import Markdown

# ============================================================================
# Task Rendering
# ============================================================================

def render_task_totals(
    console,
    total_tasks: int,
    total_results: int,
) -> None:
    """
    Print task/result totals.
    """

    console.print(
        f"[b]Total Tasks: [blue]{total_tasks}[/]"
    )

    console.print(
        f"[b]Total Results: [blue]{total_results}[/]"
    )


def render_task_list(
    console,
    tasks: list[tuple[str, int]],
) -> None:
    """
    Print task names and cached-result counts.
    """

    for task_name, result_count in tasks:

        console.print(
            f"- {task_name} ([green]{result_count}[/])"
        )


# ============================================================================
# Dependency Rendering
# ============================================================================

def render_dependency_list(
    console,
    title: str,
    task_names: list[str],
) -> None:
    """
    Print a dependency listing.
    """

    if not task_names:

        console.print(
            f"[yellow]No {title.lower()} tasks.[/]"
        )

        return

    console.print(
        f"[b]{title}[/]"
    )

    for task_name in task_names:

        console.print(
            f"  ├─ {task_name}"
        )


# ============================================================================
# Documentation Rendering
# ============================================================================

def render_task_info(
    console,
    docstring: str | None,
    autodoc: str | None,
    show_docstring: bool = True,
    show_funcs: bool = True,
    markdown: bool = True,
) -> None:
    """
    Render task documentation.
    """

    console.print()

    if show_docstring:

        if markdown:

            console.print(
                Markdown(docstring or "")
            )

        else:

            console.print(
                (docstring or "").strip()
            )

        console.print()

    if show_funcs:

        console.print(
            Markdown(
                "__Auto-documented function docs__"
            )
        )

        console.print()

        console.print(
            autodoc or ""
        )

        console.print()


# ============================================================================
# Dataset Rendering
# ============================================================================

def render_dataset_summary(
    console,
    dataset,
) -> None:
    """
    Print dataset metadata.
    """

    dtype = (
        dataset.dtype.__name__
        if dataset.dtype
        else "Any"
    )

    console.print(
        f"[b]Name:[/] {dataset.name}"
    )

    console.print(
        f"[b]Type:[/] {dtype}"
    )

    if dataset.owner:

        console.print(
            f"[b]Owner:[/] {dataset.owner}"
        )

    if dataset.source:

        console.print(
            f"[b]Source:[/] {dataset.source}"
        )

    if dataset.tags:

        console.print(
            f"[b]Tags:[/] "
            f"{', '.join(dataset.tags)}"
        )

    if dataset.doc:

        console.print()

        console.print(
            Markdown(dataset.doc)
        )


# ============================================================================
# Cache Rendering
# ============================================================================

def render_cache_listing(
    console,
    entries: list[str],
) -> None:
    """
    Print cache listing output.
    """

    for entry in entries:

        console.print(entry)