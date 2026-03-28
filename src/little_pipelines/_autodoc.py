"""
Autodoc
Auto-document tasks and the custom processes therein.

"""

import inspect
from dataclasses import dataclass
from typing import Any, Annotated, Optional, TYPE_CHECKING, get_type_hints, get_args, get_origin

if TYPE_CHECKING:
    from little_pipelines import Task


@dataclass
class Arg:
    """
    Represents a single parameter or return value from a function signature.
    """
    name: str
    type_hint: str
    doc: Optional[str] = ""
    default: Optional[Any] = None

    def as_string(self):
        return f"{self.name} ({self.type_hint}): {self.doc}"


@dataclass
class Func:
    """
    Represents a function's attributes.
    """
    parent: str
    name: str
    returns: Any
    args: list[Arg]
    doc: str = ""

    def as_string(self):
        s = (
            f"Task('{self.parent}').{self.name}\n"
            f"{self.doc}\n\nArgs:\n"
            f"{'\n'.join(['    ' + a.as_string() for a in self.args])}\n\nReturns:"
        )
        return s + "\n    " + str(self.returns) + "\n\n\n"


def unpack_anno(hint: Any) -> tuple[Any, str]:
    if get_origin(hint) is Annotated:
        return get_args(hint)
    return (hint, "")


def _autodoc(task: "Task") -> str:
    processes = {}
    for func_name, method in task.__dict__.items():
        # Filter out any attrs that are not a wrapped method (by task.process)
        if not hasattr(method, "__wrapped__"):
            continue

        func_doc = method.__doc__ or "<missing docstring>"
        hints = get_type_hints(method, include_extras=True)
        return_value = hints.get("return")
        sig = inspect.signature(method)
        args: list[Arg] = []
        for arg, type_hint in hints.items():
            if arg == "return":
                continue

            param = sig.parameters[arg]
            default = param.default if param.default is not inspect.Parameter.empty else None
            type_hint, anno = unpack_anno(type_hint)
            args.append(
                Arg(arg, inspect.formatannotation(type_hint), anno, default)
            )

        processes[func_name] = Func(task.name, func_name, return_value, args, func_doc)
    
    s = ""
    for proc in processes.values():
        s += ("-" * 25) + "\n"
        s += proc.as_string()

    # Remove all tailing newlines
    s = s.strip("\n")
    # Add one last tailing newline
    s += "\n"
    return s
