"""
Tests for Task discovery utilities.

Focus:
    - find_tasks()
    - nested module discovery
    - duplicate elimination
    - nested=False behavior
"""

from types import ModuleType

from little_pipelines import Task, find_tasks


# =============================================================================
# Direct discovery
# =============================================================================

def test_find_tasks_finds_local_tasks():
    first = Task("First")
    second = Task("Second")

    namespace = {
        "first": first,
        "second": second,
        "not_a_task": 123,
    }

    found = find_tasks(namespace)

    assert found == {
        first,
        second,
    }


def test_find_tasks_returns_empty_set_when_no_tasks_exist():
    namespace = {
        "a": 1,
        "b": object(),
        "c": "hello",
    }

    found = find_tasks(namespace)

    assert found == set()


# =============================================================================
# Nested module discovery
# =============================================================================

def test_find_tasks_finds_tasks_inside_modules():
    task = Task("Nested")

    module = ModuleType("fake_module")
    module.task = task

    namespace = {
        "module": module,
    }

    found = find_tasks(namespace)

    assert found == {
        task,
    }


def test_find_tasks_nested_false_skips_modules():
    task = Task("Nested")

    module = ModuleType("fake_module")
    module.task = task

    namespace = {
        "module": module,
    }

    found = find_tasks(
        namespace,
        nested=False,
    )

    assert found == set()


# =============================================================================
# De-duplication
# =============================================================================

def test_find_tasks_returns_unique_tasks():
    task = Task("Shared")

    namespace = {
        "first": task,
        "second": task,
    }

    found = find_tasks(namespace)

    assert found == {
        task,
    }


def test_find_tasks_deduplicates_direct_and_nested_references():
    task = Task("Shared")

    module = ModuleType("fake_module")
    module.task = task

    namespace = {
        "task": task,
        "module": module,
    }

    found = find_tasks(namespace)

    assert found == {
        task,
    }


# =============================================================================
# Robustness
# =============================================================================

def test_find_tasks_ignores_modules_without_tasks():
    module = ModuleType("fake_module")
    module.value = 123

    namespace = {
        "module": module,
    }

    found = find_tasks(namespace)

    assert found == set()


def test_find_tasks_ignores_bad_module_attributes():
    task = Task("Nested")

    class ExplodingModule(ModuleType):
        def __getattribute__(self, name):
            if name == "broken":
                raise RuntimeError("boom")
            return super().__getattribute__(name)

    module = ExplodingModule("fake_module")
    module.task = task

    namespace = {
        "module": module,
    }

    found = find_tasks(namespace)

    assert task in found
