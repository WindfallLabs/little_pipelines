import little_pipelines as lp


zero = lp.Task(
    name="Zero",
)

@zero.process
def run(this, *args, **kwargs):
    this.log(f"{this.name} is running!")
    this.store("data", ["Some", "values"])
    this.log(f"Has data: {this.has_data}")
    return


one = lp.Task(
    name="One",
    dependencies=["Zero"]
)


@one.process
def preflight(this, *args, **kwargs):
    this.log("Just checking something...")
    this.store("preflight", "OK")
    return


@one.process
def run(this, *args, **kwargs):
    this.log(f"{this.name} is running!")
    this.preflight()  # As defined above
    this.log("Let's mutate <cyan>Zero</>'s data!")
    data = lp.get_task_data("Zero", "data")
    data.extend(["more", "values"])
    #this.log(data)
    this.log("Setting a value")
    this.store("data", ["another"])
    return


manual = lp.Task(
    name="Manual Task",
    dependencies=["Zero", "One"],
    execution_type="MANUAL"  # ignored by the graph by default
)

@manual.process
def run(this, *args, **kwargs):
    this.log(f"{this.name} is running!")
    this.log("Getting <cyan>Zero</>'s data again")
    zero_data = lp.get_task_data("Zero", "data")
    one_preflight = lp.get_task_data("One", "preflight")
    if one_preflight == "OK":
        this.log("Things are good!")
    one_data = lp.get_task_data("One", "data")
    this.log(f"'{zero_data[1]}' and '{one_data[0]}'")
    return
