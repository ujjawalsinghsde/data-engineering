# DataFrame Creation Patterns

## Introduction

There are many ways to create a DataFrame in Spark.

At first this feels confusing, but each method has a purpose.

The common ways are:

- `spark.read`
- `spark.sql`
- `spark.table`
- `spark.range`
- `spark.createDataFrame`
- RDD to DataFrame conversion

The right choice depends on where the data is coming from.

## Big Picture

```text
Files / Tables / Python Objects / RDD
        |
        v
SparkSession
        |
        v
DataFrame
        |
        v
Transformations
        |
        v
Write / Table / Dashboard / ML
```

DataFrame creation is the first step in most Spark pipelines.

If data is loaded incorrectly, every later transformation becomes risky.

## Method 1: Create DataFrame Using `spark.read`

This is the most common method in production.

Use it when data is stored in:

- HDFS
- S3
- ADLS Gen2
- GCS
- local filesystem
- Parquet, CSV, JSON, ORC files

Example:

```python
orders_schema = """
order_id long,
order_date string,
customer_id long,
order_status string
"""

orders_df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/ujjawalsingh/retail_db/orders")
)

orders_df.show(5)
orders_df.printSchema()
```

Why use this:

- reads distributed files
- works with large data
- supports schema enforcement
- supports read options

When not to use:

- if data is already in a Spark table
- if data is tiny and manually created for testing

## Standard Reader API

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(path)
)
```

This is flexible because I can change format and options.

## Shortcut Reader Methods

Spark also provides shortcut methods:

```python
csv_df = spark.read.csv(path, header=True, schema=schema)
json_df = spark.read.json(path, schema=schema)
parquet_df = spark.read.parquet(path)
orc_df = spark.read.orc(path)
```

Shortcut methods are clean for simple reads.

Standard reader is better when there are many options.

## Method 2: Create DataFrame Using `spark.sql`

Any Spark SQL query returns a DataFrame.

Example:

```python
closed_orders_df = spark.sql("""
    SELECT *
    FROM orders_ext
    WHERE order_status = 'CLOSED'
""")

closed_orders_df.show()
```

Use `spark.sql` when:

- data is in a table or temp view
- transformation is easier to express in SQL
- team is comfortable with SQL
- migrating logic from Hive or database SQL

Remember:

```text
Spark SQL result = DataFrame
```

So this is valid:

```python
result_df = spark.sql("SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id")
result_df.filter("order_count > 5").show()
```

SQL and DataFrame APIs can be mixed.

## Method 3: Create DataFrame Using `spark.table`

`spark.table` reads a table directly as a DataFrame.

Example:

```python
orders_df = spark.table("retail.orders_ext")
orders_df.show()
```

This is shorter than:

```python
orders_df = spark.sql("SELECT * FROM retail.orders_ext")
```

Use `spark.table` when:

- I need the full table
- table exists in metastore/catalog
- no SQL filtering is needed immediately

## Method 4: Create DataFrame Using `spark.range`

`spark.range` creates a DataFrame with one column named `id`.

Examples:

```python
spark.range(5).show()
```

Output:

```text
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

Start and end:

```python
spark.range(0, 8).show()
```

Start, end, step:

```python
spark.range(0, 8, 2).show()
```

Output:

```text
0
2
4
6
```

Use `spark.range` for:

- testing
- generating sample data
- benchmarking
- creating sequence IDs
- learning transformations

Do not use it for business data ingestion.

## Method 5: Create DataFrame From Python List

This is useful for practice and unit testing.

Example:

```python
orders_list = [
    (1, "2013-07-25 00:00:00.0", 11599, "CLOSED"),
    (2, "2013-07-25 00:00:00.0", 256, "PENDING_PAYMENT"),
    (3, "2013-07-25 00:00:00.0", 12111, "COMPLETE"),
]

orders_raw_df = spark.createDataFrame(orders_list)
orders_raw_df.show()
orders_raw_df.printSchema()
```

If I do not give column names, Spark creates default names like:

```text
_1, _2, _3, _4
```

This is not readable.

## Add Column Names With `toDF`

```python
orders_df = (
    spark.createDataFrame(orders_list)
    .toDF("order_id", "order_date", "customer_id", "order_status")
)

orders_df.show()
```

Use this when:

- list data is small
- I only need to fix column names
- type inference is acceptable

## Create DataFrame With Column Name List

```python
orders_schema = ["order_id", "order_date", "cust_id", "order_status"]

orders_df = spark.createDataFrame(orders_list, orders_schema)
orders_df.printSchema()
```

This fixes names but still lets Spark infer data types.

## Create DataFrame With DDL Schema

```python
orders_schema = """
order_id long,
order_date string,
cust_id int,
order_status string
"""

orders_df = spark.createDataFrame(orders_list, orders_schema)
orders_df.printSchema()
```

This fixes both:

- column names
- data types

## Create DataFrame With StructType

```python
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_date", StringType(), True),
    StructField("cust_id", IntegerType(), True),
    StructField("order_status", StringType(), True),
])

orders_df = spark.createDataFrame(orders_list, orders_schema)
```

This is the production-friendly style.

## When To Use Python List DataFrames

Good use cases:

- unit tests
- examples
- small lookup/reference data
- reproducing bugs
- interview coding questions

Bad use cases:

- large datasets
- reading millions of records into driver
- replacing file-based ingestion

Important:

Python list exists on the driver first.

If the list is huge, driver memory can crash.

## Method 6: Convert RDD To DataFrame

RDD to DataFrame conversion is useful when:

- I already have legacy RDD code
- parsing logic is easier at RDD level
- source data is raw text
- I need low-level transformations first

Example:

```python
orders_rdd = spark.sparkContext.textFile(
    "/public/ujjawalsingh/retail_db/orders/part-00000"
)

new_orders_rdd = orders_rdd.map(lambda x: (
    int(x.split(",")[0]),
    x.split(",")[1],
    int(x.split(",")[2]),
    x.split(",")[3]
))
```

Now convert to DataFrame:

```python
orders_schema = """
order_id long,
order_date string,
customer_id int,
order_status string
"""

orders_df = spark.createDataFrame(new_orders_rdd, orders_schema)
orders_df.show()
```

## RDD To DataFrame Using `toDF`

```python
orders_df = (
    spark.createDataFrame(new_orders_rdd)
    .toDF("order_id", "order_date", "customer_id", "order_status")
)
```

Or:

```python
orders_df = new_orders_rdd.toDF(orders_schema)
```

## RDD To DataFrame: Things To Remember

RDD has no schema.

DataFrame has schema.

So when converting:

```text
RDD + schema = DataFrame
```

Avoid unnecessary RDD conversion in production because DataFrames are easier for Spark to optimize.

## Nested DataFrame From Local List

Example:

```python
customer_list = [
    (1, ("sumit", "mittal"), "bangalore"),
    (2, ("ram", "kumar"), "hyderabad"),
    (3, ("vijay", "shankar"), "pune"),
]

ddl_schema = """
customer_id long,
fullname struct<firstname:string, lastname:string>,
city string
"""

customer_df = spark.createDataFrame(customer_list, ddl_schema)
customer_df.printSchema()
customer_df.show(truncate=False)
```

This creates a struct column called `fullname`.

## ArrayType And StructType

In JSON data, we often see arrays.

Example:

```json
{
  "library_name": "Central Library",
  "books": [
    {"book_id": "B1", "book_name": "Spark Basics"},
    {"book_id": "B2", "book_name": "Kafka Basics"}
  ]
}
```

Here:

- `books` is an array
- each item inside `books` is a struct

Schema pattern:

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
```

Why `ArrayType`?

Because one library can have many books and many members.

If I used only `StructType`, it would mean one book or one member.

## DataFrame Creation Decision Table

| Need | Best Option |
|---|---|
| Read files from data lake | `spark.read` |
| Query table with SQL | `spark.sql` |
| Read full table | `spark.table` |
| Create test sequence | `spark.range` |
| Create tiny test data | `spark.createDataFrame` |
| Convert legacy RDD | `spark.createDataFrame(rdd, schema)` or `rdd.toDF()` |
| Nested JSON | `StructType` with `ArrayType` / nested `StructType` |

## Practical Example: Create Weather DataFrame

Question:

Create DataFrame with columns `season` and `windspeed`.

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

+------+---------+
|season|windspeed|
+------+---------+
|Spring|     12.3|
|Summer|     10.5|
|Autumn|      8.2|
|Winter|     15.1|
+------+---------+
```

## Important Points To Remember

- `spark.read` is the main production ingestion method.
- `spark.sql` returns a DataFrame.
- `spark.table` is a shortcut for reading a table.
- `spark.range` creates a one-column DataFrame called `id`.
- `spark.createDataFrame` is good for small local data and tests.
- Avoid creating huge DataFrames from Python lists.
- RDD to DataFrame needs a schema.
- For nested JSON, understand `StructType` and `ArrayType`.

## Common Mistakes

- Creating huge DataFrames from local Python lists.
- Forgetting to provide column names.
- Using RDD conversion when DataFrame reader can do the job.
- Reading CSV without schema.
- Treating nested JSON as plain string.
- Using `spark.sql` before creating a temp view or table.
- Hardcoding database names from another user's lab.

## Best Practices

- Use explicit schema for production reads.
- Use small Python lists only for tests and examples.
- Use `spark.table` for cataloged tables.
- Use `spark.sql` when the logic is naturally SQL.
- Use `printSchema()` after creating DataFrames.
- Keep sample data small and readable.
- Prefer DataFrame APIs over RDDs unless RDD is truly needed.

## Performance Tips

- File-based reads are distributed; Python lists start on driver.
- Avoid driver-heavy data creation for large datasets.
- Reading Parquet is faster than CSV for analytics.
- Avoid unnecessary RDD conversion because it can bypass some DataFrame optimizations.
- Use schema enforcement to avoid extra scanning.

## Interview Questions

### Beginner Questions

- What are different ways to create a DataFrame?
- What is the difference between `spark.sql` and `spark.table`?
- What does `spark.range` create?
- How do you create a DataFrame from a Python list?
- What is the default column name in `spark.range`?

### Intermediate Questions

- When would you convert an RDD to a DataFrame?
- Why should large DataFrames not be created from Python lists?
- How do you enforce schema while using `createDataFrame`?
- How do you create a nested DataFrame?
- What is the difference between `StructType` and `ArrayType`?

### Senior Data Engineer Questions

- How would you design a reusable DataFrame ingestion framework?
- How would you handle schema differences across multiple source files?
- When would you use catalog tables instead of direct file paths?
- How would you build test DataFrames for unit testing Spark transformations?
- What risks do you see in legacy RDD-heavy Spark pipelines?

## Scenario-Based Questions

### Scenario 1: Driver Memory Error

A developer creates a DataFrame from a Python list of 10 million records and the driver crashes.

What is wrong?

Answer:

The data is first created on the driver. Large data should be stored in distributed storage and read using `spark.read`.

### Scenario 2: SQL Query Fails With Table Not Found

Possible causes:

- temp view was not created
- wrong database selected
- table exists in another catalog/database
- session isolation issue

Debug:

```python
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()
```

## Real Project Perspective

In production, DataFrame creation is often wrapped inside ingestion utilities.

Example:

```text
Source Config
   |
   v
Schema Registry / Schema File
   |
   v
Spark Reader
   |
   v
Raw DataFrame
   |
   v
Validation
```

Good teams do not write random reads everywhere.

They standardize:

- file format
- schema
- read mode
- bad record handling
- audit columns
- logging

## Quick Revision

- `spark.read` reads files.
- `spark.sql` runs SQL and returns DataFrame.
- `spark.table` reads table directly.
- `spark.range` creates test sequence.
- `createDataFrame` creates DataFrame from local data or RDD.
- Use lists for small test data only.
- RDD to DataFrame = RDD plus schema.
- Use `ArrayType` for repeated nested values.
