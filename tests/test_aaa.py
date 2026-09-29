"""
Garin's fully-vetted tests
"""


import little_pipelines as lp


def test_common():
    cache = lp.Cache()
    the_output = lp.DataSpec("The Output", dtype=int)

    the_task = lp.Task(
        "The Task",
        outputs=[the_output]
    )

    @the_task.main
    def main(t: lp.Task) -> int:
        return (
            the_output.fulfill(42),
        )

    result = the_task.main()
    assert result == 42


def test_bigtest():
    cache = lp.Cache()

    output_a = lp.DataSpec("A", dtype=str)

    @output_a.validator
    def validate(this: "DataSpec", value):
        if value != "Result for A":
            raise this.DataSpecValidationError(f"Bad validation of A: {value}")
        return value

    output_b = lp.DataSpec("B", dtype=int)
    output_c = lp.DataSpec("C", dtype=tuple)

    # =================
    task_one = lp.Task(
        "One",
        cache=cache,
        outputs=[output_a]    
    )

    @task_one.main
    def main(task: lp.Task) -> str:
        return output_a.fulfill("Result for A")

    # =================
    task_two = lp.Task(
        "Two",
        cache=cache,
        dependencies=["One"],
        outputs=[output_b]
    )

    @task_two.main
    def main(task: lp.Task) -> int:
        a: str = task.dependencies["A"].value
        b: int = len(a)
        return output_b.fulfill(b)

    # =================
    task_three = lp.Task(
        "Three",
        cache=cache,
        dependencies=[output_b],  # and implicitly output_a
        outputs=[output_c]
    )

    @task_three.main
    def main(task: lp.Task) -> int:
        a: str = task.dependencies["A"].value
        b: int = task.dependencies["B"].value
        return output_c.fulfill((a, b))

    pipeline = lp.Pipeline(
        "The Pipeline",
        cache=cache
    )
    pipeline.add(
        task_one,
        task_two,
        task_three,
    )

    assert pipeline.dependency_graph == {"One": [], "Two": ["One"], "Three": ["Two"]}
    assert list(pipeline.topologically_sorted) == ["One", "Two", "Three"]
    assert pipeline.get_upstream("Three") == ["One", "Two"]
    assert pipeline.get_downstream("One") == ["Two", "Three"]

    pipeline.execute()

    assert task_one.is_executed is True
    assert "A" in cache.keys()
    assert task_two.is_executed is True
    assert "B" in cache.keys()
    assert task_three.is_executed is True
    assert "C" in cache.keys()

    assert output_a.get_from_cache(cache) == "Result for A"
    assert output_b.get_from_cache(cache) == 12
    assert output_c.get_from_cache(cache) == ("Result for A", 12)
