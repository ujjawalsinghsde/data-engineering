# Optimization And Assignment Notes

## Introduction

This section ends with two important themes:

1. Managed vs external table assignment.
2. Spark optimization basics at application and cluster level.

This note combines the practical assignment flow with production and interview notes.

## Assignment 1: Managed And External Tables

Tasks:

1. Create managed Spark table from CSV.
2. Create external Spark table from same CSV.
3. Verify data.
4. Drop both and observe difference.
5. Repeat with JSON.

CSV path:

```text
/public/ujjawalsingh/groceries.csv
```

JSON path:

```text
/public/ujjawalsingh/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json
```

## Setup Database

```python
db_name = f"{username}_week5_assignment"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")
spark.sql("SHOW TABLES").show()
```

Using username in database name prevents collision in shared lab.

## 1.1 Managed Table From CSV

Create table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.groceries (
        order_id STRING,
        location STRING,
        item STRING,
        order_date STRING,
        quantity INT
    )
    USING CSV
""")
```

Read CSV:

```python
groceries_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/groceries.csv")
)

groceries_df.printSchema()
groceries_df.show(5)
```

Load into managed table:

```python
groceries_df.createOrReplaceTempView("groceries_temp")

spark.sql(f"""
    INSERT INTO {db_name}.groceries
    SELECT * FROM groceries_temp
""")
```

Verify:

```python
spark.sql(f"SELECT * FROM {db_name}.groceries LIMIT 10").show()
```

## 1.2 External Table From CSV

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.groceries_external (
        order_id STRING,
        location STRING,
        item STRING,
        order_date STRING,
        quantity INT
    )
    USING CSV
    OPTIONS (header='true')
    LOCATION '/public/ujjawalsingh/groceries.csv'
""")
```

Verify:

```python
spark.sql(f"SELECT * FROM {db_name}.groceries_external LIMIT 10").show()
```

Describe:

```python
spark.sql(f"DESCRIBE EXTENDED {db_name}.groceries").show(truncate=False)
spark.sql(f"DESCRIBE EXTENDED {db_name}.groceries_external").show(truncate=False)
```

Check:

- Managed table location should be in warehouse.
- External table location should be original public path.

## 1.3 Drop CSV Tables

Drop external:

```python
spark.sql(f"DROP TABLE IF EXISTS {db_name}.groceries_external")
```

External table drop removes metadata only.

Drop managed:

```python
spark.sql(f"DROP TABLE IF EXISTS {db_name}.groceries")
```

Managed table drop removes metadata and warehouse data.

## 1.5 Managed And External Tables From JSON

Read JSON:

```python
orders_json_df = spark.read.json(
    "/public/ujjawalsingh/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json"
)

orders_json_df = orders_json_df.select(
    "order_id",
    "order_date",
    "customer_id",
    "order_status"
)
```

Create managed JSON table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_json (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING JSON
""")

orders_json_df.createOrReplaceTempView("orders_json_temp")

spark.sql(f"""
    INSERT INTO {db_name}.orders_json
    SELECT * FROM orders_json_temp
""")
```

Create external JSON table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_json_external (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING JSON
    LOCATION '/public/ujjawalsingh/orders_wh.json'
""")
```

Verify:

```python
spark.sql(f"SELECT * FROM {db_name}.orders_json LIMIT 10").show()
spark.sql(f"SELECT * FROM {db_name}.orders_json_external LIMIT 10").show()
```

Drop:

```python
spark.sql(f"DROP TABLE IF EXISTS {db_name}.orders_json_external")
spark.sql(f"DROP TABLE IF EXISTS {db_name}.orders_json")
```

## Assignment 2 And 3 Reminder

Products and customers solutions are in:

```text
03_DataFrame_And_SQL_Use_Cases.md
```

Why separated:

- Products/customers are DataFrame and SQL query practice.
- Managed/external table assignment is Spark SQL table management practice.

## Application-Level Optimization

Application-level optimization means improving code logic.

Examples:

- Use DataFrames/Spark SQL instead of RDD where possible.
- Avoid `groupByKey`; use aggregations.
- Filter early.
- Select only required columns.
- Cache reused DataFrames.
- Use broadcast join for small dimension table.
- Use Parquet instead of CSV for analytics.
- Avoid Python UDFs when built-in functions exist.

## Cluster-Level Optimization

Cluster-level optimization means giving the Spark job correct resources.

Resources:

- CPU cores.
- Memory/RAM.

Spark uses executors.

Executor is like a container/JVM with CPU and memory.

```text
Worker Node
    |
    +-- Executor 1
    +-- Executor 2
    +-- Executor 3
```

One worker node can run multiple executors.

## Thin Executor Strategy

Thin executor means many executors with small resources.

Example node:

```text
16 cores, 64 GB RAM
16 executors
Each executor = 1 core, 4 GB RAM
```

Problems:

- No multithreading inside executor.
- Too many JVMs.
- Too many copies of broadcast variables.
- More overhead.

Not recommended as default production strategy.

## Fat Executor Strategy

Fat executor means one executor gets most resources.

Example:

```text
1 executor per node
16 cores, 64 GB RAM
```

Problems:

- More than around 5 cores per executor can hurt HDFS throughput.
- Very large memory causes long garbage collection.
- Fewer executors can reduce parallelism.

Garbage collection means JVM clearing unused objects from memory.

## Balanced Executor Strategy

Course example:

```text
10 worker nodes
Each node = 16 cores, 64 GB RAM
```

Reserve per node:

```text
1 core for OS/background
1 GB RAM for OS/background
```

Available per node:

```text
15 cores
63 GB RAM
```

Choose:

```text
5 cores per executor
```

Why 5?

- More than 1 core gives multithreading.
- Not too high to hurt HDFS throughput.
- Good balance.

Executors per node:

```text
15 cores / 5 cores per executor = 3 executors per node
```

Memory per executor:

```text
63 GB / 3 = 21 GB
```

Overhead memory:

```text
max(384 MB, 7% of executor memory)
7% of 21 GB = around 1.5 GB
```

Executor memory:

```text
21 GB - 1.5 GB = around 19 GB
```

Across cluster:

```text
10 nodes * 3 executors = 30 executors
1 executor reserved for YARN Application Master
Final executors = 29
```

Final configuration:

```text
29 executors
5 cores per executor
19 GB executor memory
```

## Example Spark Submit Config

```bash
spark-submit \
  --num-executors 29 \
  --executor-cores 5 \
  --executor-memory 19G \
  --conf spark.executor.memoryOverhead=1536M \
  app.py
```

In notebooks, cluster config is often managed by lab/platform.

## Optimization In DataFrame API

### Filter Early

Good:

```python
orders_df.filter("order_status = 'CLOSED'").select("order_id", "customer_id")
```

### Select Required Columns

Good:

```python
orders_df.select("order_id", "customer_id", "order_status")
```

### Cache Reused DataFrame

```python
orders_df.cache()

orders_df.groupBy("order_status").count().show()
orders_df.select("customer_id").distinct().count()

orders_df.unpersist()
```

### Broadcast Small Table

```python
from pyspark.sql.functions import broadcast

result = orders_df.join(broadcast(customers_df), "customer_id")
```

## Explain Plan

Use:

```python
result.explain()
result.explain(True)
```

Look for:

- Scan type.
- Filters pushed down.
- BroadcastHashJoin.
- SortMergeJoin.
- Exchange, which indicates shuffle.

## Common Mistakes

- Using thin executors with 1 core each.
- Using one huge executor per node.
- Ignoring executor memory overhead.
- Caching DataFrames used only once.
- Forgetting to unpersist.
- Using `inferSchema` on very large CSV.
- Dropping managed tables carelessly.
- Not using `DESCRIBE EXTENDED` before drop.

## Best Practices

- Use DataFrame/Spark SQL for structured data.
- Use explicit schema in production.
- Use Parquet/Delta for analytical tables.
- Use external tables for shared lake data.
- Use managed tables for Spark-owned data.
- Use balanced executor sizing.
- Reserve resources for OS and Application Master.
- Watch Spark UI and explain plans.
- Optimize code before increasing cluster size.

## Interview Questions

### Beginner Questions

- What is managed table?
- What is external table?
- What happens when managed table is dropped?
- What is executor?
- What is executor memory?

### Intermediate Questions

- What is difference between application-level and cluster-level optimization?
- Why are thin executors bad?
- Why are fat executors bad?
- How do you calculate executor sizing?
- What is memory overhead?

### Senior Data Engineer Questions

- How would you size executors for a 10-node cluster?
- How do you decide managed vs external table in production?
- How do you optimize a slow Spark SQL query?
- What do you check in explain plan?
- How do you balance cost and performance?

## Scenario-Based Questions

### Scenario 1: Dropped Table, Data Gone

Question:

A user dropped a Spark table and data disappeared from warehouse. What happened?

Answer:

It was likely a managed table. Dropping managed table deletes both metadata and data.

### Scenario 2: Job Has Long GC Time

Question:

Spark job is slow and Spark UI shows high garbage collection time.

Answer:

Executor memory may be too large or memory pressure is high. Use balanced executor sizing, reduce heap size per executor, check caching, and inspect spills.

### Scenario 3: Shared Data Should Not Be Deleted

Question:

Multiple teams use the same raw data. Which table type?

Answer:

External table, because dropping table removes only metadata, not shared data.

## Quick Revision

```text
Optimization types:
1. Application code level
2. Cluster/resource level

Thin executor = too many small executors
Fat executor = one huge executor
Balanced = around 5 cores/executor

10 nodes, 16 cores, 64 GB:
reserve 1 core + 1 GB per node
15 cores, 63 GB left
3 executors/node
5 cores/executor
~19 GB executor memory
29 final executors after AM
```
