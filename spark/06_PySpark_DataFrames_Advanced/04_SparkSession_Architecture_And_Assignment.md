# SparkSession Architecture And Assignment Notes

## Introduction

SparkSession is the entry point for DataFrames and Spark SQL.

In earlier Spark versions, developers used separate contexts:

- `SparkContext` for RDDs
- `SQLContext` for SQL
- `HiveContext` for Hive integration

SparkSession unified these.

Now we mostly start with:

```python
spark = SparkSession.builder.getOrCreate()
```

## Why SparkSession Exists

SparkSession is like the main door to Spark.

Through it, I can:

- read files
- create DataFrames
- run SQL
- access tables
- access SparkContext for RDDs
- configure Spark behavior
- connect with Hive metastore

Diagram:

```text
SparkSession
    |
    +-- spark.read
    +-- spark.sql
    +-- spark.table
    +-- spark.range
    +-- spark.createDataFrame
    +-- spark.sparkContext
```

## Standard SparkSession Code

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession
    .builder
    .appName("week6-dataframe-transformations")
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

## Code Explanation

### `SparkSession.builder`

Starts the builder pattern.

Builder pattern means we build the object step by step.

### `.appName(...)`

Sets the application name.

This name appears in Spark UI and YARN.

Use meaningful names.

Bad:

```text
test
```

Better:

```text
daily-orders-cleaning
```

### `.config("spark.ui.port", "0")`

Spark UI usually uses port `4040`.

In a shared lab, many users may start Spark apps.

Using `"0"` tells Spark:

```text
pick any available port
```

This avoids port conflicts.

### `.config("spark.sql.warehouse.dir", ...)`

This is where managed Spark table data is stored.

Example:

```text
/user/<username>/warehouse
```

For managed tables:

```text
warehouse/database.db/table_name
```

### `.enableHiveSupport()`

Enables Hive metastore support.

This is needed when I want to:

- create databases
- create managed/external tables
- query Hive/Spark SQL tables
- use `spark.table`

### `.master("yarn")`

Tells Spark to run on a YARN-managed cluster.

Other options:

```python
.master("local[*]")
```

Runs locally using all available cores.

Use `local[*]` for local development.

Use `yarn` for Hadoop cluster execution.

### `.getOrCreate()`

If SparkSession already exists, return it.

If not, create a new one.

This prevents duplicate SparkSessions in notebooks.

## SparkSession And SparkContext

SparkContext still exists.

It is available inside SparkSession:

```python
sc = spark.sparkContext
```

Use `spark.sparkContext` for RDD operations:

```python
rdd = spark.sparkContext.textFile("/path/to/file")
```

Use `spark` for DataFrames and SQL:

```python
df = spark.read.csv("/path")
spark.sql("SELECT * FROM table")
```

## One SparkContext Per Application

Within one Spark application:

```text
one SparkContext
multiple SparkSessions can exist
```

Multiple SparkSessions share the same SparkContext.

Example:

```python
spark1 = SparkSession.builder.getOrCreate()
spark2 = spark1.newSession()

print(spark1.sparkContext == spark2.sparkContext)
```

Expected output:

```text
True
```

## Why Multiple SparkSessions?

Sometimes one application needs isolated SQL environments.

Example:

```python
spark1 = SparkSession.builder.getOrCreate()
spark2 = spark1.newSession()
```

Temp views created in one session are not automatically visible in another session.

This can help with:

- multi-user applications
- isolated testing
- different SQL configs
- avoiding temp view name conflicts

## Spark Application Architecture

Every Spark application has:

- one driver
- multiple executors

Diagram:

```text
Spark Application
    |
    +-- Driver
    |      |
    |      +-- SparkSession
    |      +-- SparkContext
    |      +-- DAG creation
    |
    +-- Executor 1
    +-- Executor 2
    +-- Executor 3
```

Driver:

- runs the main program
- creates SparkSession
- builds DAG
- schedules jobs
- coordinates executors

Executors:

- run tasks
- process partitions
- store cached data
- return results to driver

## Client Mode Vs Cluster Mode

## Client Mode

In client mode, driver runs on the client/gateway machine.

Diagram:

```text
Gateway / Client Node
    |
    +-- Driver
            |
            v
Cluster Executors
```

Use client mode for:

- notebooks
- development
- debugging
- interactive analysis

Risk:

If client node disconnects or notebook stops, driver can die.

## Cluster Mode

In cluster mode, driver runs inside the cluster.

Diagram:

```text
Cluster
    |
    +-- Driver
    +-- Executor 1
    +-- Executor 2
    +-- Executor 3
```

Use cluster mode for:

- production jobs
- scheduled pipelines
- long-running jobs
- Airflow/Oozie/ADF-triggered batch jobs

Why cluster mode for production?

Because the job should not depend on a user's gateway session.

## Deployment Mode Comparison

| Feature | Client Mode | Cluster Mode |
|---|---|---|
| Driver runs | client/gateway | cluster |
| Best for | dev/testing | production |
| Interactive | yes | no |
| Client disconnect impact | high | low |
| Logs | easier locally | cluster logs |

## Assignment 1: Create DataFrame With Season And Windspeed

Question:

Create a DataFrame with two columns:

- `season`
- `windspeed`

Data:

```python
[
    ("Spring", 12.3),
    ("Summer", 10.5),
    ("Autumn", 8.2),
    ("Winter", 15.1),
]
```

Solution:

```python
data = [
    ("Spring", 12.3),
    ("Summer", 10.5),
    ("Autumn", 8.2),
    ("Winter", 15.1),
]

df = spark.createDataFrame(data, schema=["season", "windspeed"])

df.printSchema()
df.show()
```

Expected output:

```text
root
 |-- season: string
 |-- windspeed: double
```

## Assignment 2: Library JSON With StructType

Path:

```text
/public/ujjawalsingh/datasets/library_data.json
```

The data contains:

- library details
- books array
- members array
- books borrowed array

Solution:

```python
from pyspark.sql.types import *

library_schema = StructType([
    StructField("library_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("books", ArrayType(
        StructType([
            StructField("book_id", StringType(), True),
            StructField("book_name", StringType(), True),
            StructField("author", StringType(), True),
            StructField("copies_available", IntegerType(), True),
        ])
    ), True),
    StructField("members", ArrayType(
        StructType([
            StructField("member_id", StringType(), True),
            StructField("member_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("books_borrowed", ArrayType(StringType()), True),
        ])
    ), True),
])

library_df = (
    spark.read
    .schema(library_schema)
    .json("/public/ujjawalsingh/datasets/library_data.json")
)

library_df.printSchema()
library_df.show(truncate=False)
```

Why `ArrayType`?

Because one library can have many books and many members.

Why `StructType` inside `ArrayType`?

Because each book/member has multiple fields.

## Assignment 3: Train Dataset Transformations

Path:

```text
/public/ujjawalsingh/datasets/train.csv
```

Tasks:

1. Drop `passenger_name` and `age`.
2. Count rows after removing duplicates using `train_number` and `ticket_number`.
3. Count unique train names.

Solution:

```python
train_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/datasets/train.csv")
)

dropped_df = train_df.drop("passenger_name", "age")

deduped_df = dropped_df.dropDuplicates(["train_number", "ticket_number"])

rows_after_dedup = deduped_df.count()

unique_train_names = deduped_df.select("train_name").distinct().count()

print("Rows after removing duplicates:", rows_after_dedup)
print("Unique train names:", unique_train_names)
```

Interview note:

`dropDuplicates(["train_number", "ticket_number"])` means Spark keeps one row for each unique combination of train number and ticket number.

## Assignment 4: Read Modes On Sales JSON

Path:

```text
/public/ujjawalsingh/datasets/sales_data.json
```

Schema:

```python
sales_schema = """
store_id integer,
product string,
quantity integer,
revenue double
"""
```

### 4.1 Permissive Mode

```python
df_permissive = (
    spark.read
    .schema(sales_schema)
    .option("mode", "permissive")
    .json("/public/ujjawalsingh/datasets/sales_data.json")
)

permissive_count = df_permissive.count()
print("Permissive records:", permissive_count)
```

### 4.2 Dropmalformed Mode

```python
df_dropmalformed = (
    spark.read
    .schema(sales_schema)
    .option("mode", "dropmalformed")
    .json("/public/ujjawalsingh/datasets/sales_data.json")
)

dropmalformed_count = df_dropmalformed.count()

dropped_records = permissive_count - dropmalformed_count

print("Dropmalformed records:", dropmalformed_count)
print("Dropped malformed records:", dropped_records)
```

Important:

This count difference is a simple learning method.

In production, use corrupt record capture/quarantine where possible.

### 4.3 Failfast Mode

```python
df_failfast = (
    spark.read
    .schema(sales_schema)
    .option("mode", "failfast")
    .json("/public/ujjawalsingh/datasets/sales_data.json")
)

df_failfast.show()
```

Expected behavior:

If malformed records exist, Spark throws an error.

## Assignment 5: Hospital Dataset

Path:

```text
/public/ujjawalsingh/datasets/hospital.csv
```

Fields:

```text
patient_id integer
admission_date date, format MM-dd-yyyy
discharge_date date, format yyyy-MM-dd
diagnosis string
doctor_id integer
total_cost float
```

Important:

The two date columns have different formats.

So a safer solution is to read both dates as strings and convert separately.

```python
from pyspark.sql.functions import to_date, expr

hospital_schema = """
patient_id integer,
admission_date string,
discharge_date string,
diagnosis string,
doctor_id integer,
total_cost float
"""

hosp_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(hospital_schema)
    .load("/public/ujjawalsingh/datasets/hospital.csv")
)

hospital_final_df = (
    hosp_df
    .withColumn("admission_date", to_date("admission_date", "MM-dd-yyyy"))
    .withColumn("discharge_date", to_date("discharge_date", "yyyy-MM-dd"))
    .drop("doctor_id")
    .withColumnRenamed("total_cost", "hospital_bill")
    .withColumn("duration_of_stay", expr("datediff(discharge_date, admission_date)"))
    .withColumn(
        "adjusted_total_cost",
        expr("""
            CASE
                WHEN diagnosis = 'Heart Attack' THEN hospital_bill * 1.5
                WHEN diagnosis = 'Appendicitis' THEN hospital_bill * 1.2
                ELSE hospital_bill
            END
        """)
    )
    .select(
        "patient_id",
        "diagnosis",
        "hospital_bill",
        "adjusted_total_cost"
    )
)

hospital_final_df.show()
```

Why I prefer this over direct date schema:

- one date format option cannot safely handle both columns if formats differ
- string-first parsing makes bad dates easier to detect
- I can add validation for null parsed dates

Validation example:

```python
bad_dates_df = hospital_final_df.filter(
    "hospital_bill IS NULL OR adjusted_total_cost IS NULL"
)

bad_dates_df.show()
```

## Production Perspective

These topics are very production-relevant.

Real pipelines spend a lot of effort on:

- schema enforcement
- corrupt record handling
- date parsing
- column cleanup
- deduplication
- nested JSON handling
- SparkSession configuration

In actual projects, a pipeline may look like this:

```text
Raw JSON/CSV Files
      |
      v
Read With Explicit Schema
      |
      v
Parse Dates And Cast Types
      |
      v
Validate Required Columns
      |
      +--> Bad Records Table
      |
      v
Clean DataFrame
      |
      v
Deduplicate
      |
      v
Write Curated Table
```

## Monitoring

For production jobs, monitor:

- input record count
- output record count
- bad record count
- null count by important columns
- duplicate count
- schema drift
- job duration
- failed tasks
- executor memory
- shuffle size

## Common Production Issues

- source adds new column
- source changes date format
- CSV has delimiter inside quoted text
- corrupt JSON appears
- duplicate files are loaded
- driver dies in client mode
- table not found due to wrong database
- `inferSchema` gives different type after source data changes

## Best Practices

- Use explicit schemas.
- Use `StructType` for nested JSON.
- Read risky date columns as string first.
- Keep bad records for audit.
- Use cluster mode for production jobs.
- Use meaningful Spark app names.
- Keep warehouse paths user-specific in shared labs.
- Stop sessions/kernels after practice.
- Avoid hardcoded personal lab usernames in reusable code.

## Interview Questions

### Beginner Questions

- What is SparkSession?
- Why do we need SparkSession?
- What is SparkContext?
- What is the difference between client mode and cluster mode?
- What does `getOrCreate()` do?

### Intermediate Questions

- How does SparkSession relate to SparkContext?
- Can one application have multiple SparkSessions?
- Why is `enableHiveSupport()` used?
- Why do we set `spark.sql.warehouse.dir`?
- What happens if the driver fails?

### Senior Data Engineer Questions

- Why is cluster mode preferred for production?
- How would you design SparkSession creation for dev, stage, and prod?
- How would you manage Spark configs across many jobs?
- What issues can happen when multiple users share the same cluster?
- How would you isolate sessions in a multi-tenant Spark application?

## Scenario-Based Questions

### Scenario 1: Notebook Job Dies When Laptop Disconnects

Likely reason:

The job was running in client mode and the driver was on the client/gateway.

Production fix:

Run in cluster mode using scheduler/orchestrator.

### Scenario 2: Managed Table Data Goes To Wrong Location

Check:

```python
spark.conf.get("spark.sql.warehouse.dir")
```

Also check:

```python
spark.sql("DESCRIBE EXTENDED database.table").show(truncate=False)
```

### Scenario 3: Temp View Not Visible

Possible reason:

Temp views are session-scoped.

If created in `spark1`, they may not be visible in `spark2`.

Use global temp view or persistent table if cross-session access is needed.

## Quick Revision

- SparkSession is the unified entry point.
- SparkContext is still used for RDDs.
- One Spark application has one SparkContext.
- Multiple SparkSessions can share one SparkContext.
- Driver coordinates work.
- Executors process partitions.
- Client mode is good for development.
- Cluster mode is better for production.
- `enableHiveSupport()` enables metastore/table access.
- `spark.sql.warehouse.dir` controls managed table storage.
