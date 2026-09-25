# Little Pipelines

## tl;dr
A small, Python-native, local-first data processing toolkit built specifically for solo analysts or small teams who need more structure than just a collection of scripts (but not the complexity of enterprise orchestration platforms).  


## Details

Our goal is to provide a small set of tools that analysts can use to build reliable and intuitive data processing workflows that are entirely Python-driven.  
Little Pipelines centers around these simple objects:  

- **Data**: documented dataset definitions
- **Task**: basic units of work; function-based
- **Cache**: SQLite-backed storage for artifacts (Results) that created by and shared between Tasks
- **Pipeline**: Task orchestrator and dependency manager
- **Shell**: an interactive shell/workspace


## Examples


### Example 1: Simple Task, if not a little stupid

___FAILS___
```python

import little_pipelines as lp


# Initialize a cache to persist results
cache = lp.Cache()

# Define the task
hello = lp.Task(
    "Hello",
    cache=cache,  # Persist the results
)


# Define the work that the task will do
@hello.main
def main(task):
    return "Hello World"


# Create the pipeline and add the Task
pipeline = lp.Pipeline("Example Pipeline")
pipeline.add(hello)

# Execute the pipeline
# This sorts Tasks and calls the `main` method of each (like defined above)
pipeline.execute()

# Get the result of the task
r: lp.Result = cache.get("Hello")

print(r.data)  # "Hello World"

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

pipeline.add(
    load_sales,
    summarize_sales,
)

pipeline.execute()

print(cache.get("SummarizeSales").data)  # "{'records': 3, 'total_sales': 525}"

```


### Example 3: Best practice, if not over-engineered

```python

import pandas as pd

import little_pipelines as lp


# ============================================================
# Data Definitions

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


# ============================================================
# Shared Cache

cache = lp.Cache()


# ============================================================
# Task 1

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
# Task 2

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
# Pipeline

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

df: pd.DataFrame = cache.get("MonthlyRidership").data
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



