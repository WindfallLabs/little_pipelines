![Tests](dev/tests-badge.svg)
![Coverage Status](dev/coverage-badge.svg)
[![AI-DECLARATION: pair](https://img.shields.io/badge/䷼%20AI--DECLARATION-pair-ffedd5)](https://ai-declaration.md)


# Little Pipelines

## tl;dr
A small, Python-native, local-first data processing toolkit built specifically for solo analysts or small teams who need more structure than just a collection of scripts (but not the complexity of enterprise orchestration platforms).  


## Details

Our goal is to provide a small set of tools that analysts can use to build reliable and intuitive data processing workflows that are entirely Python-driven.  
Little Pipelines centers around these simple objects:  

- **Data**: Documented dataset definitions
- **Task**: Basic units of work; function-based
- **Cache**: SQLite-backed storage for artifacts (Results) that created by and shared between Tasks
- **Pipeline**: Task orchestrator and dependency manager
- **Shell**: An interactive shell/workspace; customizable via subclassing


## Little Pipelines in action

### Example 1: Simple Task, if not a little stupid

```python

import little_pipelines as lp


# Initialize a cache to persist results
cache = lp.Cache()

# Define the task
hello = lp.Task(
    "Hello",
    cache=cache,  # Persist the results
)


# Define the work that the task will do as a wrapped function
@hello.main  # Essentially makes the function a method, with added magic
def main(task):
    return "Hello World"  # Return some result


# Create the pipeline and add the Task
pipeline = lp.Pipeline("Example Pipeline")
pipeline.add(hello)

# Execute the pipeline
# This sorts Tasks and calls the `main` method of each (like defined above)
pipeline.execute()

# Get the result of the task
r: lp.Result = cache.get("Hello")

print(r.data)  # Unpack the data from the Result: "Hello World"

```


### Example 2: Two Tasks, mild complexity

```python

import little_pipelines as lp

cache = lp.Cache()

load_sales = lp.Task(
    "LoadSales",
    cache=cache,
)

@load_sales.main
def main(task):
    return lp.Data(task.name).fulfill([
        {"amount": 100},
        {"amount": 250},
        {"amount": 175},
    ])


summarize_sales = lp.Task(
    "SummarizeSales",
    cache=cache,
    dependencies=["LoadSales"],
)


@summarize_sales.main
def main(task):
    # Load the Result of another Task
    sales = task.dependencies["LoadSales"].data

    total = sum(row["amount"] for row in sales)

    # Return named results
    return lp.Data(task.name).fulfill({
        "records": len(sales),
        "total_sales": total,
    })


pipeline = lp.Pipeline(
    "SalesSummary",
    cache=cache,
)

# The order in which you add Tasks doesn't matter
pipeline.add(
    load_sales,
    summarize_sales,
)

pipeline.execute()

print(cache.get("SummarizeSales").data)  # "{'records': 3, 'total_sales': 525}"

```


### Example 3: Best practice, if not over-engineered

___NOTE: this is currently executing PrepareRidership twice___

```python

# ============================================================
# my_cache.py

import little_pipelines as lp

cache = lp.Cache()


# ============================================================
# my_data.py

import pandas as pd
import little_pipelines as lp

#from my_cache import cache


raw_ridership = lp.Data(
    "RawRidership",
    dtype=pd.DataFrame,
    doc="Raw APC export from the transit agency."
)

monthly_ridership = lp.Data(
    "MonthlyRidership",
    dtype=pd.DataFrame,
    doc="Cleaned monthly ridership totals."
)


@monthly_ridership.getter
def get(self):
    # This is a lazy way but works
    return cache.get("MonthlyRidership").data


# ============================================================
# my_tasks/prepare_ridership.py

import pandas as pd

import little_pipelines as lp

#from my_cache import cache
#from my_data import raw_ridership


prepare_ridership = lp.Task(
    "PrepareRidership",
    cache=cache,
    # Set the optional expected ouput(s) for validation
    outputs=[raw_ridership],
)


@prepare_ridership.process
def extract(task):
    return pd.DataFrame(
        {
            "month": ["2025-01", "2025-01", "2025-02"],
            "boardings": [1200, 950, 1300],
        }
    )


@prepare_ridership.process
def transform(task, df):
    return (
        df.groupby("month", as_index=False)
          .agg({"boardings": "sum"})
    )


@prepare_ridership.process
def export(task, df):
    # Non-blocking print statement
    task.logger.print("Prepared raw ridership table")
    return df


@prepare_ridership.main
def main(task):
    # Execute the subprocesses
    raw = task.extract()
    transformed = task.transform(raw)
    exported = task.export(transformed)

    # Return the fulfilled expected ouput
    return raw_ridership.fulfill(exported)


# ============================================================
# my_tasks/build_ridership_report.py

import little_pipelines as lp

#from my_cache import cache


build_report = lp.Task(
    "BuildRidershipReport",
    cache=cache,
    # Defines that this Task requires Data as processed by some other Task
    dependencies=[raw_ridership],
    # Set the optional expected ouput(s) for validation
    outputs=[monthly_ridership],
)


@build_report.process
def extract(task):
    return task.dependencies["RawRidership"].data


@build_report.process
def transform(task, df):
    df = df.copy()

    df["change_pct"] = (
        df["boardings"]
        .pct_change()
        .fillna(0)
        * 100
    )
    # Display the status spinner during long processes
    import time
    time.sleep(2)

    return df


@build_report.process
def export(task, df):
    # Print log message
    task.logger.warn("Exported monthly ridership report")
    return df


@build_report.main
def main(task):
    # Execute the subprocesses
    source = task.extract()
    transformed = task.transform(source)
    report = task.export(transformed)

    # Return the fulfilled expected ouput
    return monthly_ridership.fulfill(report)


# ============================================================
# my_tasks/__init__.py

#from prepare_ridership.py import prepare_ridership
#from build_ridership_report.py import build_report


# ============================================================
# pipeline.py

import little_pipelines as lp

#from my_cache import cache
#from my_tasks import build_report, prepare_ridership


pipeline = lp.Pipeline(
    "RidershipReporting",
    cache=cache,
)

pipeline.add(
    prepare_ridership,
    build_report,
)

pipeline.execute()


# ============================================================
# Proof

import pandas as pd

#from my_data import monthly_ridership


# Use the user-defined getter
df: pd.DataFrame = monthly_ridership.get()
print(df)

```




A typical workflow might combine:

- Python data processing
- SQL transformations
- GIS analysis
- File operations
- API requests
- Reporting workflows

into a single, reproducible pipeline.



