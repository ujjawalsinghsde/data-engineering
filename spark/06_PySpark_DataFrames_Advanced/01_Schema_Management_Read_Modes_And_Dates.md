# Schema Management, Read Modes, And Dates

## Introduction

In earlier weeks I used `inferSchema = true` many times because it is quick for learning.

But in real projects, schema inference is usually not the right choice.

Schema means the structure of data:

- column names
- data types
- nullable or non-nullable fields
- nested fields, if the data is JSON or complex

Example:

```text
order_id      long
order_date    date
customer_id   long
order_status  string
```

A DataFrame is not just data. It is data plus schema.

That schema is what allows Spark to optimize queries, validate columns, and give us a table-like view.

## Why Schema Management Matters

In production, schema is a contract.

It tells the pipeline:

- what columns are expected
- what type each column should be
- how corrupt values should be handled
- whether a file is safe to process

Without schema control, pipelines become fragile.

Example:

```text
2013-07-25
07-25-2013
25-07-2013
```

All three look like dates to humans, but Spark needs an exact format.

If Spark expects `yyyy-MM-dd` and the data is `MM-dd-yyyy`, parsing can fail and values may become `null`.

## Schema Inference

Schema inference means Spark looks at the data and guesses the data types.

Example:

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/orders_wh/*")
)
```

This is convenient during practice.

But the problem is that Spark must scan data to guess the schema.

For a small file, this is fine.

For a 5 TB file, this is expensive.

## Problems With Schema Inference

### 1. Wrong Data Type

Spark may infer the wrong type.

Examples:

- a date may be inferred as `string`
- an ID may be inferred as `integer`, but should be `string`
- a column with mostly numbers and a few bad values may be inferred incorrectly
- a decimal amount may be inferred as `double`, causing precision issues

In data engineering, IDs should often be strings even if they look numeric.

Example:

```text
zipcode = 00725
```

If Spark reads this as integer, it becomes:

```text
725
```

That is wrong because zip code is not a number used for calculation.

It is an identifier.

### 2. Performance Cost

Spark has to scan the data before loading it properly.

This adds extra time.

If the data is huge, schema inference can become a real bottleneck.

### 3. Inconsistent Results

If different files have different values, Spark may infer different schemas at different times.

Example:

```text
day 1 file: customer_id values are all numbers
day 2 file: one customer_id has ABC123
```

Now schema behavior can change.

This is dangerous for scheduled pipelines.

## Sampling Ratio

Spark can infer schema by scanning only a sample of data.

Example:

```python
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("samplingRatio", 0.01)
    .load("/public/yelp-dataset/yelp_user.csv")
)
```

This means Spark uses around 1% of the data to infer schema.

This improves speed, but it can make correctness worse.

If the sample does not contain edge cases, Spark may infer the wrong type.

Use it for exploration, not for production pipelines.

## Schema Enforcement

Schema enforcement means I tell Spark the schema explicitly.

Spark does not guess.

This is better for production because:

- faster read
- predictable behavior
- fewer silent surprises
- easier debugging
- cleaner data contracts

There are two common ways:

1. DDL string schema
2. `StructType` schema

## Method 1: DDL String Schema

DDL means Data Definition Language.

It is SQL-like schema syntax.

Example:

```python
orders_schema = """
order_id long,
order_date date,
cust_id long,
order_status string
"""

df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/ujjawalsingh/datasets/orders_sample1.csv")
)

df.printSchema()
df.show()
```

Expected schema:

```text
root
 |-- order_id: long
 |-- order_date: date
 |-- cust_id: long
 |-- order_status: string
```

Use DDL schema when:

- schema is simple
- data is flat
- I want quick readable code
- I am writing notebook-level code

Avoid DDL schema when:

- schema is deeply nested
- schema is reused across many jobs
- I need stronger structure and maintainability

## Method 2: StructType Schema

`StructType` is the programmatic way to define schema.

Example:

```python
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, StringType

orders_schema_struct = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True),
])

df = (
    spark.read
    .format("csv")
    .schema(orders_schema_struct)
    .load("/public/ujjawalsingh/datasets/orders_sample1.csv")
)

df.printSchema()
df.show()
```

Each `StructField` has:

```text
column name
data type
nullable flag
```

Example:

```python
StructField("order_id", LongType(), True)
```

This means:

- column name is `order_id`
- type is long
- null values are allowed

Use `StructType` when:

- schema is complex
- schema is nested
- schema needs to be reused
- code is production-grade
- we want clear schema versioning

## What Happens If Data Type Does Not Match?

If Spark cannot parse a value into the given data type, the value may become `null` in permissive mode.

Example:

```python
wrong_schema = """
order_id long,
order_date date,
cust_id long,
order_status long
"""

df = (
    spark.read
    .format("csv")
    .schema(wrong_schema)
    .load("/public/ujjawalsingh/datasets/orders_sample1.csv")
)

df.show()
```

Here `order_status` contains values like `CLOSED` and `COMPLETE`.

Spark cannot convert `CLOSED` into long.

So `order_status` can become `null`.

Important point:

Spark may not always loudly fail. Sometimes it quietly gives nulls.

That is why data quality checks are important.

## Handling Dates

Dates are one of the most common sources of bugs.

Spark default date format:

```text
yyyy-MM-dd
```

Example:

```text
2013-07-25
```

If the input date is already in this format, reading it as `date` is simple.

```python
orders_schema = """
order_id long,
order_date date,
cust_id long,
order_status string
"""

df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/ujjawalsingh/datasets/orders_sample1.csv")
)
```

## Custom Date Format

If date is in this format:

```text
07-25-2013
```

Then we must tell Spark the format:

```python
orders_schema = """
order_id long,
order_date date,
cust_id long,
order_status string
"""

df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .option("dateFormat", "MM-dd-yyyy")
    .load("/public/ujjawalsingh/datasets/orders_sample2.csv")
)
```

Very important:

```text
MM = month
mm = minutes
```

So this is wrong:

```python
.option("dateFormat", "mm-dd-yyyy")
```

This is correct:

```python
.option("dateFormat", "MM-dd-yyyy")
```

## Better Date Strategy: Load As String First

In real pipelines, I often prefer loading date columns as string first.

Then I convert them after basic validation.

Why?

- I can inspect bad date values.
- I can handle multiple formats.
- I can avoid losing raw values immediately.
- Debugging becomes easier.

Example:

```python
from pyspark.sql.functions import to_date

orders_schema = """
order_id long,
order_date string,
cust_id long,
order_status string
"""

df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/ujjawalsingh/datasets/orders_sample2.csv")
)

new_df = df.withColumn(
    "order_date",
    to_date("order_date", "MM-dd-yyyy")
)

new_df.printSchema()
new_df.show()
```

Here `withColumn` modifies the existing `order_date` column.

If I use a new column name:

```python
new_df = df.withColumn(
    "order_date_new",
    to_date("order_date", "MM-dd-yyyy")
)
```

then Spark creates a new column instead.

## Read Modes

Read mode tells Spark what to do when it sees bad or malformed records.

Three common modes:

```text
permissive
failfast
dropmalformed
```

## Permissive Mode

`permissive` is the default mode.

Behavior:

- Spark continues reading.
- Bad fields may become `null`.
- The job does not fail immediately.

Example:

```python
orders_schema = """
order_id long,
order_date string,
cust_id long,
order_status string
"""

df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .option("mode", "permissive")
    .load("/public/ujjawalsingh/datasets/orders_sample3.csv")
)

df.show()
```

Use permissive when:

- doing data exploration
- source quality is unknown
- I want to inspect bad data
- pipeline should not fail immediately

Be careful:

Permissive mode can hide problems if nobody checks null counts.

## Failfast Mode

`failfast` stops the job as soon as Spark sees malformed data.

Example:

```python
df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .option("mode", "failfast")
    .load("/public/ujjawalsingh/datasets/orders_sample3.csv")
)

df.show()
```

Use failfast when:

- data must be 100% clean
- pipeline feeds finance, medical, regulatory, or critical reporting
- bad records should block the batch

Do not use failfast when:

- source commonly sends minor bad records
- business wants partial load with quarantine
- streaming job should stay alive

## Dropmalformed Mode

`dropmalformed` drops bad records and continues.

Example:

```python
df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .option("mode", "dropmalformed")
    .load("/public/ujjawalsingh/datasets/orders_sample3.csv")
)

df.show()
```

Use dropmalformed when:

- bad records are not useful
- business allows dropping invalid rows
- downstream needs only clean records

Be careful:

Dropping records silently can cause wrong reports.

Always count dropped records and monitor the trend.

## Production Pattern For Bad Records

In production, a better design is:

```text
Raw Input
   |
   v
Read As String / Permissive
   |
   v
Validate Columns
   |
   +--> Good Records -> Clean Table
   |
   +--> Bad Records  -> Quarantine Table
```

This avoids silently losing data.

Example:

```python
from pyspark.sql.functions import col, to_date

raw_df = (
    spark.read
    .format("csv")
    .schema("order_id string, order_date string, cust_id string, order_status string")
    .load("/public/ujjawalsingh/datasets/orders_sample2.csv")
)

validated_df = raw_df.withColumn(
    "parsed_order_date",
    to_date("order_date", "MM-dd-yyyy")
)

good_df = validated_df.filter(col("parsed_order_date").isNotNull())
bad_df = validated_df.filter(col("parsed_order_date").isNull())
```

## Nested Schema

Nested schema means one column itself contains structure.

Example JSON:

```json
{"customer_id":1,"fullname":{"firstname":"sumit","lastname":"mittal"},"city":"bangalore"}
{"customer_id":2,"fullname":{"firstname":"ram","lastname":"kumar"},"city":"hyderabad"}
```

Here `fullname` is not a simple string.

It contains:

```text
firstname
lastname
```

## Nested Schema Using DDL

```python
ddl_schema = """
customer_id long,
fullname struct<firstname:string, lastname:string>,
city string
"""

df = (
    spark.read
    .format("json")
    .schema(ddl_schema)
    .load("/public/ujjawalsingh/datasets/customer_nested/*")
)

df.printSchema()
df.show(truncate=False)
```

Expected schema:

```text
root
 |-- customer_id: long
 |-- fullname: struct
 |    |-- firstname: string
 |    |-- lastname: string
 |-- city: string
```

## Nested Schema Using StructType

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType

customer_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("fullname", StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
    ]), True),
    StructField("city", StringType(), True),
])

df = (
    spark.read
    .format("json")
    .schema(customer_schema)
    .load("/public/ujjawalsingh/datasets/customer_nested/*")
)
```

## Accessing Nested Fields

```python
df.select(
    "customer_id",
    "fullname.firstname",
    "fullname.lastname",
    "city"
).show()
```

Or rename nested output:

```python
from pyspark.sql.functions import col

flattened_df = df.select(
    col("customer_id"),
    col("fullname.firstname").alias("first_name"),
    col("fullname.lastname").alias("last_name"),
    col("city")
)
```

## Important Points To Remember

- Avoid `inferSchema` in production.
- Use explicit schema for predictable pipelines.
- DDL schema is simple and readable.
- `StructType` is better for nested and production schemas.
- Spark date format is case-sensitive.
- `MM` means month, `mm` means minutes.
- If parsing fails, values may become null.
- Read modes decide how Spark handles malformed records.
- Dropping malformed rows without counting them is risky.

## Common Mistakes

- Using `inferSchema` on huge files.
- Treating zip code or ID as integer.
- Using `mm-dd-yyyy` instead of `MM-dd-yyyy`.
- Not checking null counts after parsing dates.
- Using `dropmalformed` and not tracking how many rows were dropped.
- Defining nested JSON columns as string when they should be struct or array.
- Assuming all bad data will fail the job. In permissive mode, it may not.

## Best Practices

- Keep schemas in reusable variables or config files.
- Version schemas when source data changes.
- Load risky date fields as string first.
- Quarantine bad records instead of silently dropping them.
- Add data quality checks after reading.
- Use `StructType` for production code.
- Use `printSchema()` during development.
- Use `df.show(truncate=False)` when inspecting nested values.

## Performance Tips

- Schema enforcement is faster than inference.
- Avoid scanning large data just to infer types.
- Prefer Parquet/Delta/ORC for analytical pipelines because schema is stored with the data.
- Read only required columns when possible.
- Validate early so bad data does not travel through the full pipeline.

## Interview Questions

### Beginner Questions

- What is a schema in Spark?
- What is schema inference?
- Why is `inferSchema` not recommended in production?
- What is the difference between DDL schema and `StructType`?
- What is Spark's default date format?

### Intermediate Questions

- What happens when Spark cannot parse a value into the specified type?
- Explain permissive, failfast, and dropmalformed modes.
- Why should IDs sometimes be read as strings?
- How do you handle custom date formats in Spark?
- How do you define a nested schema for JSON?

### Senior Data Engineer Questions

- How would you design schema evolution for a production pipeline?
- How would you handle bad records without losing data?
- What data quality checks would you add after reading raw files?
- How would you manage schema contracts between upstream and downstream teams?
- When would you intentionally use permissive mode in production?

## Scenario-Based Questions

### Scenario 1: Date Column Becomes Null

Your date column is full of nulls after reading a CSV.

How would you troubleshoot?

Checklist:

- Check raw date format.
- Check `dateFormat`.
- Verify `MM` vs `mm`.
- Load the date column as string.
- Use `to_date` with the correct format.
- Count records where parsed date is null.

### Scenario 2: Pipeline Suddenly Fails

Yesterday the job worked. Today it fails while reading CSV.

Possible reasons:

- new malformed record arrived
- source schema changed
- delimiter changed
- date format changed
- header changed
- file contains extra columns

Follow-up:

- Would you fail the pipeline or quarantine bad records?
- How would you alert the source team?

## Real Project Perspective

In production, schema management is part of data governance.

Good pipelines usually have:

- raw zone where data is stored as received
- schema validation layer
- clean zone with enforced schema
- bad records/quarantine table
- alerts for schema drift
- dashboards for null counts and malformed records

Schema problems are not small issues.

They can break dashboards, ML models, downstream joins, and business reports.

## Quick Revision

- DataFrame = data + schema.
- Do not infer schema in production.
- DDL schema is compact.
- `StructType` is best for complex schemas.
- Dates need exact format.
- `MM` is month, `mm` is minutes.
- `permissive` keeps going.
- `failfast` fails immediately.
- `dropmalformed` drops bad records.
- Bad records should usually be quarantined, not ignored.
