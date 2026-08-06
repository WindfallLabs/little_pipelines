# Little Pipelines - A Lightweight Task Pipeline Framework

__This document is a work-in-progress__  

Little Pipelines is a Python library for building and executing data pipelines. It provides an intuitive approach to orchestrating tasks while maintaining minimal dependencies and complexity. Its decorator syntax allows users to mix ETL operations, SQL transformations, API calls, file operations, and whatever else needs to get done without having to   

This library is intended for individual analysts or small teams who need simple data processing pipelines without the complexities or costs of enterprise tools or cloud-based ETL platforms (e.g., Luigi, Airflow, Prefect, dbt, Dagster, etc.).  

Free and Open Source under the [MIT License](https://mit-license.org/), and built with the love and support of the [Missoula Urban Transporation District](https://mountainline.com/about/).  


## Key Features

- __Minimal 3rd party library dependencies__ - 
- __Intelligent Execution__ - Tasks may conditionally execute based on input files, script hashes, and freshness of cached data
- __Automatic dependency resolution__ - Declare Task dependencies by name and let the pipeline handle execution order
- __Declarative task/process definitions__ - Define Pythonic Task functions without worrying about what's going on under the hood
- __Cached results__ - Cache the results and control when they expire
- __Built-in performance tracking__ - Process execution timing for each task's execution
- __Optional interactive shell__ - Subclassable `Shell` class for building custom CLI tools (work-in-progress)


## Some Super Simple Examples

### Example 1

```python
from time import sleep

import little_pipelines as lp


cache = lp.Cache()


# =====================================
# Task 1

task1 = lp.Task("TaskOne", cache=cache)

@task1.main
def main(this):
    sleep(0.1)  # Emulate processing time
    return 1


# =====================================
# Task 2

task2 = lp.Task("TaskTwo", cache=cache)

@task2.process
def extract(this):
    """A sub-process"""
    sleep(1.5)  # Emulate processing time
    return 1

@task2.process
def transform(this, ext: int):
    """A sub-process"""
    sleep(1)  # Emulate processing time
    return ext + 1

@task2.main
def main(this):
    raw_data = this.extract()
    data = this.transform(raw_data)
    return data


# =====================================
# Task 3

task3 = lp.Task(
    "TaskThree",
    cache=cache
)

@task3.main
def main(this):
    sleep(.3)  # Emulate processing time
    return 3


# =====================================
# Combine them all

sum_task = lp.Task(
    "Sum",
    dependencies=["TaskOne", "TaskTwo", "TaskThree"],
    cache=cache
)

@sum_task.main
def main(this):
    sleep(.25)  # Emulate processing time
    r1 = this.dependencies["TaskOne"].data
    r2 = this.dependencies["TaskTwo"].data
    r3 = this.dependencies["TaskThree"].data
    return r1 + r2 + r3


# =====================================
# Run it

pipeline = lp.Pipeline("Example1", cache=cache)
pipeline.add(task1, task2, task3, sum_task)
pipeline.execute()

print("Final Result: ", pipeline.cache.get("Sum")[0].data)  # 6

```


## The Big Picture

In short, Little Pipelines lets you mix ETL operations, SQL transformations, Python data processing, API calls, and file operations in a single pipeline. First, users define `Tasks` and `add()` them to a `Pipeline`.  
Under the hood, the Pipeline coordinates Task execution using Python's `graphlib.TopologicalSorter`, and handles the caching of results using. Tasks are automatically configured with a [loguru](https://loguru.readthedocs.io/en/stable/overview.html) file-logger which logs to `.little_pipelines/<pipeline_name>/logs` (located in your user or home directory).  

Tasks can have dependencies (require the execution of other tasks before it). Dependencies are explicitly listed by name at the initialization of Tasks. Dependency management (topological sorting) is done automatically by the Pipeline instances's `tasks` property. So you could skip the built-in `execute()` method and hack something as dead-simple as:

```python
for task in pipeline.tasks:
    task.main()
```

## Some Comparisons

### Little Pipelines vs. Luigi

___Luigi___
```python
import luigi


class Task1(luigi.Task):
    def run(self):
        with self.output().open('w') as f:
            f.write('1')

    def output(self):  # Save output for other processes
        return luigi.LocalTarget('output.txt')


class Task2(luigi.Task):
    def requires(self):
        return Task1()  # Set the upstream dependency
    
    def run(self):
        # Read the output from WaitAndReturn
        with self.input().open('r') as f:
            value = int(f.read())
        
        # Do something with it
        result = value + 1
        
        with self.output().open('w') as f:
            f.write(str(result))
    
    def output(self):
        return luigi.LocalTarget('processed_output.txt')
        # NOTE: if using SqliteTarget there's a lot of boilerplate to get parody with little pipelines


if __name__ == '__main__':
    luigi.build([Task2()], local_scheduler=True)  # Run the tasks
    print(int(Task2().output().open().read()))

```

___Little Pipelines___
```python
import little_pipelines as lp

# A universal, in-memory or on-disk place for storing task outputs
cache = lp.Cache()

# A task
task1 = lp.Task(
    "TaskOne",
    cache=cache
)


@task1.main   # Decorator syntax provides a "classless" API
def main(this):
    return 1  # Save output for other processes (handled silently using the cache)


# A task dependent on the execution of another task
task2 = lp.Task(
    "TaskTwo",
    cache=cache,
    dependencies=["TaskOne"]  # Specify dependencies
)


@task2.main
def main(this):
    value = this.dependencies["TaskOne"].data  # Access the result of another upstream task
    return value + 1


if __name__ == '__main__':
    pipeline = lp.Pipeline(
        "MyPipeline",  # Pipeline name
        cache=cache    # Access to the shared cache
    )
    pipeline.add(task2, task1)  # Order doesn't matter
    pipeline.execute()  # Run it (this topologically sorts tasks)
    print(pipeline.cache.get("TaskTwo")[0].data)

```
