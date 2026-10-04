# DataFrames And Spark SQL Fundamentals

## Introduction

This section starts higher-level APIs in Apache Spark.

Until now, I worked mostly with RDDs.

RDDs are powerful, but they are low-level.

RDD data is raw distributed data. It does not have column names, data types, or table-like structure by default.

Higher-level Spark APIs are:

- DataFrames.
- Spark SQL.

There is also Dataset API in Scala and Java, but in PySpark we mainly use DataFrames and Spark SQL.

Simple hierarchy:

```text
RDD
  |
  v
DataFrame
  |
  v
Spark SQL
```

DataFrames and Spark SQL are preferred in production because Spark understands the structure of the data and can optimize execution.

## RDD Vs DataFrame Vs Spark SQL

### RDD

RDD is raw distributed data.

Example:

```text
"1,2013-07-25 00:00:00.0,11599,CLOSED"
```

Spark does not automatically know:

- Column names.
- Data types.
- Which column is order ID.
- Which column is customer ID.

I manually split strings:

```python
rdd.map(lambda x: x.split(",")[3])
```

### DataFrame

DataFrame is like an RDD with schema.

Schema means structure:

```text
order_id      integer
order_date    string
customer_id   integer
order_status  string
```

DataFrame gives a tabular view:

```text
+--------+---------------------+-----------+------------+
|order_id|order_date           |customer_id|order_status|
+--------+---------------------+-----------+------------+
|1       |2013-07-25 00:00:00.0|11599      |CLOSED      |
+--------+---------------------+-----------+------------+
```

### Spark SQL

Spark SQL allows SQL queries on structured data.

Example:

```sql
SELECT order_status, COUNT(*)
FROM orders
GROUP BY order_status;
```

Spark SQL table can be persistent if created in metastore.

## Database Table Analogy

A database table has two things:

```text
1. Data
2. Metadata / Schema
```

Example table: `orders`

Data:

```text
1,2013-07-25,11599,CLOSED
```

Metadata:

```text
order_id integer
order_date string/date
customer_id integer
order_status string
```

When I run:

```sql
SELECT order_id, order_date FROM orders;
```

Database combines data and metadata to return a tabular result.

If I query a column that does not exist, I get an analysis error.

Spark DataFrames and Spark SQL follow a similar idea.

## DataFrame Vs Spark Table

### DataFrame

DataFrame is temporary.

It exists only within the Spark application/session.

If the Spark application stops, the DataFrame is gone.

DataFrame metadata is held temporarily in Spark's session catalog.

### Spark Table

Spark table can be persistent.

It stores:

- Data files in storage like HDFS/S3/ADLS.
- Metadata in metastore.

Spark table can be accessed across sessions if metastore is enabled.

Comparison:

| Feature | DataFrame | Spark Table |
|---|---|---|
| Lifetime | Session/application scoped | Persistent |
| Metadata | Temporary catalog | Metastore |
| Access | Same Spark session | Across sessions |
| Query style | DataFrame API | SQL |
| Storage | Data already in source/memory plan | Table location |

Important:

```text
DataFrames are temporary.
Spark tables are persistent.
```

## Why Higher-Level APIs Are Faster

DataFrames and Spark SQL give Spark more information.

Spark knows:

- Column names.
- Data types.
- Filters.
- Selected columns.
- Join columns.
- Aggregations.

Because Spark understands the query, it can optimize it.

Main optimizers:

- Catalyst Optimizer.
- Tungsten execution engine.

### Catalyst Optimizer

Catalyst creates and optimizes query plans.

It can do:

- Predicate pushdown.
- Column pruning.
- Constant folding.
- Join optimization.
- Reordering operations.

### Tungsten

Tungsten improves:

- Memory management.
- CPU efficiency.
- Binary data processing.
- Code generation.

Interview explanation:

RDD tells Spark "run this function." DataFrame tells Spark "this is structured data and this is the operation." More context means better optimization.

## SparkSession

SparkSession is entry point for DataFrames and Spark SQL.

Boilerplate:

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession.builder
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

Explanation:

- `spark.ui.port = 0`: chooses available Spark UI port.
- `spark.sql.warehouse.dir`: location for managed table data.
- `enableHiveSupport()`: enables metastore/table support.
- `master("yarn")`: use YARN as resource manager.
- `getOrCreate()`: create or reuse SparkSession.

## Reading DataFrames

Standard reader pattern:

```python
orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/orders_wh/*")
)
```

This means:

- Format is CSV.
- First row has column names.
- Spark infers data types.
- Load files from path.

Show data:

```python
orders_df.show()
```

Show schema:

```python
orders_df.printSchema()
```

## Reader Shortcut Methods

CSV:

```python
orders_df = spark.read.csv(
    "/public/ujjawalsingh/orders_wh/*",
    header="true",
    inferSchema="true"
)
```

JSON:

```python
orders_df = spark.read.json("/public/ujjawalsingh/datasets/orders.json")
```

Parquet:

```python
orders_df = spark.read.parquet("/public/ujjawalsingh/datasets/ordersparquet")
```

ORC:

```python
orders_df = spark.read.orc("/path/to/orc")
```

Table:

```python
orders_df = spark.read.table("orders")
```

Common reader methods:

```text
csv
json
parquet
orc
jdbc
table
```

## File Formats

### CSV

CSV is plain text and human-readable.

Good for:

- Small files.
- Data exchange.
- Simple source files.

Not ideal for:

- Big analytical workloads.
- Strong schema handling.
- Compression and performance.

### JSON

JSON is semi-structured.

Good for:

- API data.
- Nested/semi-structured data.

Not ideal when:

- Huge scale analytics need columnar performance.

### Parquet

Parquet is a columnar file format.

Columnar means data is stored column-wise, not row-wise.

Why Parquet is good for Spark:

- Embedded schema.
- Good compression.
- Reads only required columns.
- Works well for analytics.
- Supports predicate pushdown.

Example:

If query needs only `customer_id` and `order_status`, Spark can avoid reading unnecessary columns.

### ORC

ORC is another optimized columnar format, common in Hive ecosystem.

## InferSchema

Course uses:

```python
.option("inferSchema", "true")
```

This is fine for learning.

But in production, avoid relying on `inferSchema`.

Problems:

- Spark scans data to infer types.
- Extra time.
- May infer wrong type.
- Schema may change unexpectedly.

Better production approach:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True),
])

orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(orders_schema)
    .load("/public/ujjawalsingh/orders_wh/*")
)
```

## Basic DataFrame Transformations

### Rename Column

```python
transformed_df = orders_df.withColumnRenamed("order_status", "status")
```

### Add Or Convert Column

```python
from pyspark.sql.functions import to_timestamp

transformed_df = transformed_df.withColumn(
    "order_date_new",
    to_timestamp("order_date")
)
```

### Select Columns

```python
selected_df = orders_df.select("order_id", "customer_id", "order_status")
```

### Filter / Where

`filter` and `where` are same in DataFrame API.

```python
filtered_df = orders_df.where("customer_id = 11599")
filtered_df.show(truncate=False)
```

or:

```python
filtered_df = orders_df.filter("customer_id = 11599")
```

Using column expressions:

```python
from pyspark.sql.functions import col

filtered_df = orders_df.filter(
    (col("customer_id") == 11599) & (col("order_status") == "CLOSED")
)
```

## DataFrame To Temp View

Convert DataFrame to SQL view:

```python
orders_df.createOrReplaceTempView("orders")
```

Then query with SQL:

```python
filtered_df = spark.sql("""
    SELECT *
    FROM orders
    WHERE order_status = 'CLOSED'
""")
```

## Temp View Types

### `createTempView`

Creates a temp view.

Fails if view already exists.

```python
orders_df.createTempView("orders")
```

### `createOrReplaceTempView`

Creates view or replaces existing one.

```python
orders_df.createOrReplaceTempView("orders")
```

This is commonly used in notebooks.

### Global Temp View

Global temp views are accessible across sessions within same Spark application.

They are stored under `global_temp` database.

```python
orders_df.createOrReplaceGlobalTempView("orders")

spark.sql("SELECT * FROM global_temp.orders").show()
```

## SQL View To DataFrame

```python
ordersdf = spark.read.table("orders")
```

or:

```python
ordersdf = spark.sql("SELECT * FROM orders")
```

## Actions, Transformations, And Utilities

### Transformations

Lazy operations that return a new DataFrame:

- `filter`
- `where`
- `select`
- `groupBy().count()`
- `orderBy`
- `distinct`
- `join`

Note:

`groupBy().count()` is a transformation because it returns a DataFrame.

### Actions

Trigger execution:

- `show`
- `count`
- `collect`
- `take`
- `head`
- `tail`
- write operations

Note:

`df.count()` is an action because it returns a number to driver.

### Utility Functions

Not transformation or action:

- `printSchema`
- `cache`
- `createOrReplaceTempView`
- `explain`

## DataFrame API Vs Spark SQL

DataFrame API:

```python
result = orders_df.groupBy("customer_id").count().sort("count", ascending=False).limit(15)
```

Spark SQL:

```python
orders_df.createOrReplaceTempView("orders")

result = spark.sql("""
    SELECT customer_id, COUNT(order_id) AS count
    FROM orders
    GROUP BY customer_id
    ORDER BY count DESC
    LIMIT 15
""")
```

Both usually create similar optimized plans.

Use DataFrame API when:

- Building programmatic pipelines.
- Need dynamic column logic.
- Want compile-time-ish function support.

Use Spark SQL when:

- Team is SQL-heavy.
- Query logic is easier in SQL.
- Analysts need to understand transformations.

## Common Mistakes

- Thinking DataFrame is permanent.
- Confusing temp view with managed table.
- Using `inferSchema` blindly in production.
- Querying column names that do not exist.
- Calling `collect()` on large data.
- Forgetting `createOrReplaceTempView` before SQL query.
- Assuming CSV has strong schema.

## Best Practices

- Use DataFrames/Spark SQL over RDDs for structured processing.
- Define schema explicitly in production.
- Use Parquet/ORC for analytics.
- Filter early.
- Select only required columns.
- Use temp views for notebook exploration.
- Use managed/external tables for persistence.
- Use `show(truncate=False)` when inspecting long columns.

## Interview Questions

### Beginner Questions

- What is a DataFrame?
- How is DataFrame different from RDD?
- What is Spark SQL?
- What is SparkSession?
- What is a temp view?

### Intermediate Questions

- Why are DataFrames faster than RDDs?
- What is Catalyst Optimizer?
- Why should `inferSchema` be avoided in production?
- What is the difference between `filter` and `where`?
- What is the difference between DataFrame and Spark table?

### Senior Data Engineer Questions

- When would you still use RDDs?
- How do DataFrames help with optimization?
- How do you design schema management in production?
- What file format would you choose for a data lake and why?
- How do you debug a DataFrame query plan?

## Quick Revision

```text
RDD = raw distributed data
DataFrame = RDD + schema
Spark SQL = SQL interface over structured data

DataFrame = temporary/session scoped
Spark table = persistent/metastore backed

DataFrame -> temp view:
df.createOrReplaceTempView("orders")

Temp view -> DataFrame:
spark.read.table("orders")

Use Parquet for analytics
Avoid inferSchema in production
```
