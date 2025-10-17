"""Console"""

from rich.console import Console

console = Console()  # TODO: something more globally available


def cprint(msg) -> None:  # TODO: ignore redefine
    console.print(msg)
    return


def warn(msg) -> None:
    console.print(f"[yellow]WARNING: {msg}[/]")
    return


__all__ = ["console", "cprint", "warn"]
