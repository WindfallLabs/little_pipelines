from pytest import fixture
import little_pipelines as lp

from .conftest import pipeline_with_tasks


@fixture
def create_shell(pipeline_with_tasks):
    pipeline, zero, one = pipeline_with_tasks

    class TestShell(lp.Shell):
        title = "Test Shell"
        header = "Header"

    return TestShell()


def test_shell_create(pipeline_with_tasks, create_shell):
    """Create a shell"""
    pipeline, zero, one = pipeline_with_tasks
    shell = create_shell

    # ...

    assert shell.title == "Test Shell"
    assert shell.header == "Header"
    assert all([shell.do_exit(""), shell.do_quit(""), shell.do_q("")])


def test_shell_run(pipeline_with_tasks, create_shell):
    """Create a shell"""
    pipeline, zero, one = pipeline_with_tasks
    shell = create_shell
    shell.set_pipeline(pipeline)

    assert shell.preloop() is None
    assert shell.postloop() is None


def test_shell_inspect(pipeline_with_tasks, create_shell):
    """Create a shell"""
    pipeline, zero, one = pipeline_with_tasks
    shell = create_shell
    shell.set_pipeline(pipeline)

    assert shell._listify_tasks() == [
        "- Zero ([yellow]not cached[/])",
        "- One ([yellow]not cached[/])",
        "[bright_black]Total: 2[/]",
    ]
    assert shell._list_cache() == [
        "[bright_black]Total: 0[/]",
    ]

    shell._execute()

    assert shell._listify_tasks() == [
        "- Zero ([green]cached[/])",
        "- One ([green]cached[/])",
        "[bright_black]Total: 2[/]",
    ]
    assert shell._list_cache() == [
        "- 'One'",
        "- 'Zero'",
        "[bright_black]Total: 2[/]",
    ]
    assert shell._list_cache("--all") == [
        "- 'One'",
        "- 'One_hashes'",
        "- 'Zero'",
        "- 'Zero_hashes'",
        "[bright_black]Total: 4[/]",
    ]


def test_shell_execute_pipeline(pipeline_with_tasks, create_shell):
    """Create a shell"""
    pipeline, zero, one = pipeline_with_tasks
    shell = create_shell

    assert zero.is_executed == False
    assert one.is_executed == False

    shell.set_pipeline(pipeline)
    shell._execute()

    assert zero.is_executed == True
    assert one.is_executed == True

    