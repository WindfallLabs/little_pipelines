"""
Tests for Task._resultify.
Claude Sonnet 5.5

Adjust the three imports below to match your package layout.

Notes:
- Outputs are declared by assigning lightweight stand-ins to `task._outputs`
  (anything with a `.name`), so these tests don't depend on how DataSpec is
  constructed. `_resultify` only needs the names.
- `make_result` assumes `Result(value=..., name=..., task_name=...)` works and
  that `task_name` may be None. Adjust if your Result differs.
"""

from types import SimpleNamespace

import pytest

from little_pipelines import exc
from little_pipelines.caching import Result
from little_pipelines.task import Task


# ============================================================================
# Helpers / fixtures


def make_result(name, value=1, task_name=None):
    return Result(value=value, name=name, task_name=task_name)


def declare_outputs(task, *names):
    """Declare expected output names without needing real DataSpec objects."""
    task._outputs = [SimpleNamespace(name=n) for n in names]
    return task


@pytest.fixture
def task():
    return Task("MyTask")


class AmbiguousEq:
    """
    Mimics numpy arrays / DataFrames: `==` returns an object whose truth
    value is ambiguous. The old `return_values == tuple()` check blew up on these.
    """

    class _Ambiguous:
        def __bool__(self):
            raise ValueError("The truth value is ambiguous")

    def __eq__(self, other):
        return self._Ambiguous()

    __hash__ = None


# ============================================================================
# Single Result


class TestSingleResult:
    def test_wrapped_in_tuple(self, task):
        r = make_result("a")
        assert task._resultify(r) == (r,)

    def test_missing_task_name_is_filled(self, task):
        r = make_result("a", task_name=None)
        (out,) = task._resultify(r)
        assert out.task_name == "MyTask"

    def test_existing_task_name_is_kept(self, task):
        r = make_result("a", task_name="Upstream")
        (out,) = task._resultify(r)
        assert out.task_name == "Upstream"

    def test_same_object_is_returned(self, task):
        r = make_result("a")
        (out,) = task._resultify(r)
        assert out is r


# ============================================================================
# Collections of Results


class TestCollectionsOfResults:
    @pytest.mark.parametrize("container", [list, tuple])
    def test_results_are_returned_as_tuple(self, task, container):
        a, b = make_result("a"), make_result("b")
        out = task._resultify(container([a, b]))
        assert isinstance(out, tuple)
        assert out == (a, b)

    @pytest.mark.parametrize("container", [list, tuple])
    def test_missing_task_names_are_filled(self, task, container):
        a = make_result("a", task_name=None)
        b = make_result("b", task_name="Other")
        out = task._resultify(container([a, b]))
        assert out[0].task_name == "MyTask"
        assert out[1].task_name == "Other"

    def test_order_is_preserved(self, task):
        results = [make_result(n) for n in "zyx"]
        out = task._resultify(results)
        assert [r.name for r in out] == ["z", "y", "x"]

    @pytest.mark.parametrize("container", [list, tuple])
    def test_mixed_results_and_values_raises(self, task, container):
        with pytest.raises(TypeError, match="mix of Result and non-Result"):
            task._resultify(container([make_result("a"), 42]))

    def test_mixed_with_value_first_raises(self, task):
        with pytest.raises(TypeError):
            task._resultify([42, make_result("a")])


# ============================================================================
# Single plain values (anything that isn't Result-like)


class TestSingleValue:
    @pytest.mark.parametrize(
        "value",
        [
            42,
            3.14,
            "a string",
            b"some bytes",
            {"a": 1},
            {1, 2, 3},
            [1, 2, 3],  # plain list must NOT be treated as a Results collection
            (1, 2, 3),  # plain tuple likewise
            range(3),
            [],
        ],
    )
    def test_value_is_wrapped_in_result(self, task, value):
        (out,) = task._resultify(value)
        assert isinstance(out, Result)
        assert out.value == value

    def test_named_after_task_when_no_outputs_declared(self, task):
        (out,) = task._resultify(123)
        assert out.name == "MyTask"
        assert out.task_name == "MyTask"

    def test_plain_list_is_one_result_not_many(self, task):
        out = task._resultify([1, 2, 3])
        assert len(out) == 1
        assert out[0].value == [1, 2, 3]

    def test_ambiguous_equality_object_does_not_raise(self, task):
        # Regression: `return_values == tuple()` raised ValueError for
        # arrays/DataFrames.
        value = AmbiguousEq()
        (out,) = task._resultify(value)
        assert out.value is value

    def test_ambiguous_equality_object_with_outputs_declared(self, task):
        declare_outputs(task, "df")
        value = AmbiguousEq()
        (out,) = task._resultify(value)
        assert out.value is value

    @pytest.mark.parametrize("falsy", [0, False, "", 0.0])
    def test_falsy_values_are_not_treated_as_missing(self, task, falsy):
        declare_outputs(task, "x")
        (out,) = task._resultify(falsy)
        assert out.value == falsy


# ============================================================================
# Naming of single values when outputs are declared


class TestSingleValueNaming:
    def test_named_after_sole_declared_output(self, task):
        declare_outputs(task, "df")
        (out,) = task._resultify([1, 2, 3])
        assert out.name == "df"
        assert out.task_name == "MyTask"

    def test_named_after_task_when_multiple_outputs_declared(self, task):
        declare_outputs(task, "a", "b")
        (out,) = task._resultify(99)
        assert out.name == "MyTask"

    def test_sole_output_matching_bare_return_passes_validation(self, task):
        # The reason for the naming rule: a bare return with one declared
        # output should not produce a spurious "Missing output" error.
        declare_outputs(task, "df")
        results = task._resultify(99)
        # _validate_outputs only checks names first; type validation calls
        # spec.validate, which our stand-in lacks, so check names directly.
        assert {r.name for r in results} == set(task.outputs)


# ============================================================================
# Nothing returned


class TestNothingReturned:
    def test_none_with_no_outputs_becomes_none_result(self, task):
        (out,) = task._resultify(None)
        assert out.value is None
        assert out.name == "MyTask"

    def test_none_with_outputs_raises_missing_output(self, task):
        declare_outputs(task, "a")
        with pytest.raises(exc.MissingOutputError, match="MyTask"):
            task._resultify(None)

    def test_empty_tuple_with_outputs_raises_missing_output(self, task):
        declare_outputs(task, "a")
        with pytest.raises(exc.MissingOutputError):
            task._resultify(())

    def test_empty_tuple_with_no_outputs_returns_empty_tuple(self, task):
        assert task._resultify(()) == ()

    def test_empty_list_is_a_value_not_missing(self, task):
        # Only an empty *tuple* counts as "nothing returned"; [] is a value.
        declare_outputs(task, "a")
        (out,) = task._resultify([])
        assert out.value == []
        assert out.name == "a"


# ============================================================================
# Duplicate names


class TestDuplicates:
    def test_duplicate_names_raise(self, task):
        with pytest.raises(exc.DuplicateResultsError):
            task._resultify([make_result("a"), make_result("a")])

    def test_error_message_names_the_duplicate(self, task):
        with pytest.raises(exc.DuplicateResultsError, match="'a'"):
            task._resultify([make_result("a"), make_result("b"), make_result("a")])

    def test_all_duplicated_names_are_reported(self, task):
        with pytest.raises(exc.DuplicateResultsError) as info:
            task._resultify(
                [make_result(n) for n in ("a", "b", "a", "b", "c")]
            )
        message = str(info.value)
        assert "'a'" in message
        assert "'b'" in message
        assert "'c'" not in message

    def test_distinct_names_do_not_raise(self, task):
        out = task._resultify([make_result("a"), make_result("b")])
        assert len(out) == 2
