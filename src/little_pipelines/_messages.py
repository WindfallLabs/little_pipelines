"""
Message formatter.
"""

import datetime as dt

from rich.console import Console


class Message():
    def __init__(self, pipeline, spaces=1, quiet=False):
        self.console = Console()
        self.console.quiet = quiet
        self.pipeline = pipeline
        self._spaces = spaces + 1
        self._time_fmt = "%Y-%m-%d %H:%M:%S.%f"
        self._time_color = "bright_black"
        self.last_task = ""
        self._quiet: bool = quiet

    @property
    def time(self):
        t = dt.datetime.now().strftime(self._time_fmt)[:-3]
        return t

    def write(self, task: str = "", msg: str = "", level: str = "INFO", task_color="grey", level_color="grey", msg_color="grey"):
        msg = str(msg)
        self.last_task = task
        _time = f"[{self._time_color}][{self.time}][/]"
        _task = "  " + f"[{task_color}]" + task.ljust(self._spaces) + "[/]"
        _level = f"[{level_color}]" + " :" + level.center(6) + ": " + "[/]"
        _msg = f"[{msg_color}]" + msg + "[/]"
        out = _time + _task + _level + _msg
        if self.pipeline is None:
            self.console.print(out)
            return
        # Pipelines handle the message queue, without a pipeline, no messages are printed
        self.pipeline._msg_queue.put(out)
        return

    def close(self, *args, **kwargs):
        #self.pipeline._msg_queue.put(exc)
        #self.pipeline._msg_queue.put(None)  # sentinel to join the process
        self.pipeline._stop_msg_thread()
        #print("Closed queue")
        #raise exc


SHELL = {
    "task": "",
    "level": "INFO",
    "task_color": "blue",
    "level_color": "blue",
    "msg_color": "blue",
}

SHELL_COMPLETE = {
    "task": "",
    "level": "OK",
    "task_color": "blue",
    "level_color": "blue",
    "msg_color": "green",
}

SHELL_FAIL = {
    "task": "",
    "level": "FAIL",
    "task_color": "blue",
    "level_color": "red bold",
    "msg_color": "red bold",
}

TASK_START = {
    "level": "EXEC",
    "task_color": "blue",
    "level_color": "blue",
    "msg_color": "blue",
}

PROCESS_START = {
    "level": "EXEC",
    "task_color": "bright_black",
    "level_color": "grey",
    "msg_color": "grey",
}

INFO = {
    "level": "INFO",
    "task_color": "bright_black",
    "level_color": "bright_black",
    "msg_color": "bright_black",
}

WARN = {
    "level": "WARN",
    "task_color": "bright_black",
    "level_color": "yellow",
    "msg_color": "yellow",
}

FAIL = {
    "level": "FAIL",
    "task_color": "red",
    "level_color": "red",
    "msg_color": "red bold",
}

PROCESS_COMPLETE = {
    "level": "DONE",
    "task_color": "bright_black",
    "level_color": "bright_black",
    "msg_color": "bright_black"
}

TASK_COMPLETE = {
    "level": "DONE",
    "task_color": "bright_black",
    "level_color": "green",
    "msg_color": "bright_black bold"
}

PIPELINE_COMPLETE = {
    "level": "DONE",
    "task_color": "green bold",
    "level_color": "green bold",
    "msg_color": "grey",
}

tasks = [
    "First Task",
    "A Second Task",
    "Another Task Name",
    "A Failure",
]


def test():
    m = Message(None, max([len(i) for i in tasks]))

    task = tasks[0]
    m.write(task, **START)  # Start task
    m.write(task, "extract", **INFO)
    m.write(task, "extract (completed in 0:00)", **PROCESS_COMPLETE)
    m.write(task, "transform", **INFO)
    m.write(task, "transform (completed in 1:00)", **PROCESS_COMPLETE)
    m.write(task, "run (completed in 1:00)", **TASK_COMPLETE)

    task = tasks[1]
    m.write(task, **START)
    m.write(task, "Message", **PROCESS_COMPLETE)

    task = tasks[2]
    m.write(task, **START)
    m.write(task, "Skipping (used cached data)", **WARN)

    task = tasks[3]
    m.write(task, **START)
    m.write(task, "ERROR MESSAGE", **FAIL)

    m.write("Pipeline Complete", "(finished in 1:00)", **PIPELINE_COMPLETE)
