# DataFrame Writer, Partitioning, And Bucketing

## Introduction

Most Spark batch pipelines follow this flow:

```text
Read Data
   |
   v
Transform / Optimize
   |
   v
Write Data
```

DataFrame reader is used to load data.

DataFrame writer is used to save data.

This section focuses on the write side:

- write formats
- write modes
- output file behavior
- partitioning
- partition pruning
- bucketing
- partitioning vs bucketing

## Data Sources

Spark can read from internal and external sources.

## Internal Sources

Internal means data is already in a data lake or distributed storage that Spark can directly read.

Examples:

- HDFS
- Amazon S3
- Azure ADLS Gen2
- Google Cloud Storage
- local filesystem for development

Example:

```python
orders_df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/trendytech/orders/orders_1gb.csv")
)
```

## External Sources

External means Spark needs a connector/driver/configuration to connect.

Examples:

- Oracle
- MySQL
- PostgreSQL
- Cassandra
- MongoDB
- Snowflake

## Two Ways To Process External Data

### Approach 1: Ingest First, Then Process

```text
MySQL / Oracle
      |
      v
ADF / Sqoop / Informatica / Glue
      |
      v
Data Lake
      |
      v
Spark Processing
```

Use when:

- data is large
- repeated processing is needed
- source database should not be overloaded
- audit/history is required
- batch processing is standard

This is usually preferred in production.

### Approach 2: Spark Reads External Source Directly

```text
Spark
  |
  v
JDBC / Connector
  |
  v
External Database
```

Use when:

- data is small
- quick lookup is needed
- one-time analysis
- direct connector is stable

Be careful:

Spark can overload OLTP databases if many partitions query them in parallel.

## Read Modes Recap

Read modes handle malformed input records:

- `permissive`
- `dropMalformed`
- `failFast`

Write modes are different. They handle output path/table existence.

## DataFrame Writer API

General pattern:

```python
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "/user/<username>/output_folder") \
    .save()
```

Important:

When reading, I can provide a file path or folder path.

When writing, I normally provide a folder path.

Spark writes one or more part files inside that folder.

## Write Formats

Common formats:

- CSV
- JSON
- Parquet
- ORC
- Avro

## CSV

CSV is human-readable, but not ideal for Spark analytics.

Problems:

- no embedded schema
- slower parsing
- larger storage
- weak support for nested data
- column pruning is limited

Use CSV for:

- simple exports
- interoperability
- small files

Avoid CSV for:

- production analytical tables
- repeated Spark processing

## JSON

JSON is good for semi-structured data.

But it can be bulky because every record stores field names.

Example:

```json
{"order_id":1,"status":"CLOSED"}
{"order_id":2,"status":"COMPLETE"}
```

Use JSON when:

- data is nested
- source system produces JSON
- schema is flexible

Avoid JSON for heavy analytics if Parquet/Delta is available.

## Parquet

Parquet is columnar and highly compatible with Spark.

Benefits:

- embedded schema
- compression
- column pruning
- predicate pushdown
- efficient analytical queries

Parquet is usually the best default for Spark batch output.

Example:

```python
orders_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", f"/user/{username}/orders_parquet") \
    .save()
```

## ORC

ORC is also columnar and optimized.

It is common in Hive-heavy ecosystems.

Parquet is usually more common with Spark, but ORC is also strong.

## Avro

Avro is row-based and schema-based.

It is common in streaming/Kafka ecosystems.

In some Spark clusters, Avro may require additional packages/configuration.

## Write Modes

Spark supports four common write modes:

```text
overwrite
append
ignore
errorIfExists
```

## `overwrite`

If output path exists, replace it.

```python
orders_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"/user/{username}/sparkwriterdemo1") \
    .save()
```

Use when:

- rebuilding full dataset
- batch output should replace old version

Be careful:

Overwrite can delete existing output.

## `append`

Adds new files to existing folder/table.

```python
orders_df.write \
    .format("parquet") \
    .mode("append") \
    .option("path", output_path) \
    .save()
```

Use when:

- adding daily/hourly incremental data
- schema is compatible

Be careful:

Appending duplicate batches can create duplicate data.

## `ignore`

If output path exists, do nothing.

```python
df.write.mode("ignore").parquet(output_path)
```

Use when:

- idempotent write is acceptable
- skip if output already exists

## `errorIfExists`

Default behavior.

If output exists, fail.

```python
df.write.mode("errorIfExists").parquet(output_path)
```

Use when:

- accidental overwrite must be prevented
- pipeline should fail if target already exists

## Number Of Output Files

Spark writes one output file per partition, per task.

Example:

```text
Input DataFrame partitions = 9
Write output files ~= 9 part files
```

If I do:

```python
orders_df.rdd.getNumPartitions()
```

and it returns:

```text
9
```

Then writing may create around 9 part files.

To control output file count:

```python
orders_df.coalesce(1).write.parquet(path)
```

or:

```python
orders_df.repartition(20).write.parquet(path)
```

Be careful with `coalesce(1)` for large data. It forces one task and can be slow or fail.

## PartitionBy

`partitionBy` organizes output data into folders based on column values.

Example:

```python
orders_df.write \
    .format("csv") \
    .mode("overwrite") \
    .partitionBy("order_status") \
    .option("path", f"/user/{username}/partition_demo_output1") \
    .save()
```

Output structure:

```text
partition_demo_output1/
    order_status=CLOSED/
    order_status=COMPLETE/
    order_status=PENDING_PAYMENT/
    order_status=PROCESSING/
```

If there are 9 distinct order statuses, there will be 9 partition folders.

## Partition Pruning

Partition pruning means Spark skips irrelevant folders.

Example:

```sql
SELECT COUNT(*)
FROM orders
WHERE order_status = 'CLOSED'
```

If data is partitioned by `order_status`, Spark reads only:

```text
order_status=CLOSED/
```

It skips other status folders.

This improves performance.

## When Partitioning Helps

Partitioning helps when:

- filter is frequently applied on partition column
- column has low to medium cardinality
- each partition has enough data
- queries can skip many partitions

Good partition columns:

- date
- year/month/day
- country/state
- order_status
- region

## When Partitioning Does Not Help

Partitioning does not help if query filters on non-partition column.

Example:

Data partitioned by `order_status`.

Query:

```sql
SELECT COUNT(*)
FROM orders
WHERE cust_id = 8827
```

Spark may still scan all order_status folders because `cust_id` is not the partition column.

## High Cardinality Problem

High cardinality means many distinct values.

Bad partition column examples:

- customer_id
- order_id
- email
- phone number

If I partition by `customer_id`, I may create millions of tiny folders.

This creates small file and metadata problems.

## Multi-Level Partitioning

Example:

```python
customers_final_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("customer_state", "customer_city") \
    .option("path", f"/user/{username}/partition_demo_output2") \
    .save()
```

Output:

```text
customer_state=CA/
    customer_city=Los Angeles/
    customer_city=San Diego/
customer_state=TX/
    customer_city=Dallas/
    customer_city=Austin/
```

This can help queries like:

```sql
WHERE customer_state = 'PR'
  AND customer_city = 'Caguas'
```

But if filtering only by city:

```sql
WHERE customer_city = 'Caguas'
```

Spark may need to search through many state folders.

Partition order matters.

## Bucketing

Bucketing organizes data into a fixed number of files based on hash of a column.

Partitioning creates folders.

Bucketing creates files/buckets.

Example:

```python
customers_final_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .bucketBy(4, "customer_id") \
    .saveAsTable(f"{db_name}.customers_bucketed")
```

Important:

Bucketing requires saving as a Spark table using `saveAsTable`.

## How Bucketing Works

If bucket count is 4, Spark applies hash logic on the bucket column.

Simple idea:

```text
bucket = hash(customer_id) % 4
```

Simplified modulo example:

```text
customer_id 1 -> bucket 1
customer_id 2 -> bucket 2
customer_id 3 -> bucket 3
customer_id 4 -> bucket 0
customer_id 5 -> bucket 1
```

If query filters:

```sql
WHERE customer_id = 10
```

Spark can identify the bucket that may contain customer_id 10 and skip other buckets.

## Bucketing Benefits

Bucketing helps with:

1. Skipping irrelevant bucket files for bucket column filters.
2. Join optimization when both tables are bucketed on join key with compatible bucket count.

Example:

```sql
SELECT *
FROM customers_bucketed
WHERE customer_id = 10
```

This can scan fewer files than non-bucketed data.

## Bucketing For Joins

If two large tables are bucketed by the same join key:

```text
orders bucketed by customer_id
customers bucketed by customer_id
```

Spark can reduce shuffle during join because matching keys are already organized similarly.

This is useful for repeated joins on large tables.

## Partitioning Vs Bucketing

| Feature | Partitioning | Bucketing |
|---|---|---|
| Physical structure | folders | files/buckets |
| Best for | low-cardinality filter columns | high-cardinality columns |
| Number of groups | based on distinct values | fixed bucket count |
| Good for dates/status | yes | less common |
| Good for customer_id/order_id | usually no | yes |
| Helps joins | sometimes | yes, if both sides bucketed |
| Requires table | no | usually yes with `saveAsTable` |

## Partitioning Plus Bucketing

Possible:

```python
df.write \
    .partitionBy("order_status") \
    .bucketBy(4, "order_id") \
    .saveAsTable("db.orders_partitioned_bucketed")
```

Physical idea:

```text
order_status=CLOSED/
    bucket files
order_status=COMPLETE/
    bucket files
```

Partitioning first creates folders.

Bucketing creates files inside folders.

Bucketing then partitioning is not the natural structure because folders can contain files, but files cannot contain folders.

## File Count With Partitioning And Bucketing

Suppose:

```text
DataFrame partitions = 16
Bucket count = 4
Partition column values = 9 statuses
```

File count can grow quickly.

Too many files cause:

- slow reads
- NameNode/metastore pressure
- higher planning overhead
- small file problem

Design partitioning and bucketing carefully.

## Practical Bucketing Example

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()
db_name = f"{username}_bucketing_db"

customers_df = (
    spark.read
    .format("csv")
    .option("inferSchema", True)
    .load("/public/trendytech/retail_db/customers/part-00000")
)

customers_final_df = customers_df.toDF(
    "customer_id",
    "customer_fname",
    "customer_lname",
    "customer_email",
    "customer_password",
    "customer_street",
    "customer_city",
    "customer_state",
    "customer_zipcode"
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

customers_final_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .bucketBy(4, "customer_id") \
    .saveAsTable(f"{db_name}.customers_bucketed")
```

Query bucket column:

```python
spark.sql(f"""
    SELECT *
    FROM {db_name}.customers_bucketed
    WHERE customer_id = 10
""").show()
```

Query non-bucket column:

```python
spark.sql(f"""
    SELECT *
    FROM {db_name}.customers_bucketed
    WHERE customer_state = 'TX'
""").show()
```

The customer_id query benefits more from bucketing.

## Common Mistakes

- Partitioning by high-cardinality columns like customer_id.
- Creating too many tiny partition folders.
- Expecting partitioning to help filters on non-partition columns.
- Forgetting write output path is a folder, not a file.
- Using CSV as long-term analytical format.
- Using `coalesce(1)` on large data.
- Hardcoding lab usernames in output paths.
- Assuming bucketing works without saving as a table.

## Best Practices

- Write analytics data in Parquet or ORC.
- Partition by low-cardinality frequently filtered columns.
- Use bucketing for high-cardinality join/filter columns.
- Keep output files reasonably sized.
- Avoid too many nested partition levels.
- Use user-specific paths in shared labs.
- Validate output file count and size.
- Use `mode("overwrite")` carefully.

## Interview Questions

### Beginner Questions

- What is DataFrame Writer API?
- What are Spark write modes?
- What is partitionBy?
- What is partition pruning?
- What is bucketing?

### Intermediate Questions

- When would you use partitioning?
- When would you use bucketing?
- Why should we avoid partitioning by customer_id?
- How does file format affect performance?
- Why is Parquet preferred over CSV?

### Senior Data Engineer Questions

- How would you design partitioning for a 10 TB orders table?
- How do you avoid small files in Spark output?
- How does bucketing optimize joins?
- What are trade-offs of multi-level partitioning?
- How would you choose between partitioning, bucketing, and Z-ordering/lakehouse optimizations?

## Scenario-Based Questions

### Scenario 1: Query On Status Is Slow

If users frequently filter by `order_status`, write data partitioned by `order_status`.

But check cardinality first.

If there are only 9 statuses, partitioning is reasonable.

### Scenario 2: Millions Of Customer Folders

Someone partitioned by `customer_id`.

Problem:

- too many folders
- small files
- slow listing
- metadata pressure

Better:

- bucket by `customer_id`
- or partition by date and bucket by customer_id

## Quick Revision

- Writer path is a folder.
- Output files depend on DataFrame partitions.
- Write modes: overwrite, append, ignore, errorIfExists.
- Parquet is usually best for Spark analytics.
- Partitioning creates folders.
- Bucketing creates fixed bucket files.
- Partition low-cardinality filter columns.
- Bucket high-cardinality join/filter columns.
