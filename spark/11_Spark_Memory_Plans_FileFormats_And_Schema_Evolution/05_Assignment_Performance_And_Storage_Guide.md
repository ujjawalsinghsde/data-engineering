# Assignment Guide: Performance, Memory, Plans, File Formats, And Schema Evolution

## 1. Goal

This assignment is open-ended. Choose a dataset from the public lab folder and demonstrate:

- Spark code development
- `spark-submit`
- executor sizing experiments
- executor memory distribution
- storage and execution memory behavior
- Hash Aggregate vs Sort Aggregate
- logical and physical plan optimization
- schema evolution
- file format comparison
- compression comparison

## 2. Suggested Dataset

You can choose any dataset under the public lab area.

Good choices:

```text
/public/trendytech/orders/orders_1gb.csv
/public/trendytech/retail_db/orders
/public/trendytech/retail_db/customers
/public/trendytech/datasets/order_data.csv
```

Use one dataset for performance experiments and another if needed for joins.

## 3. Example Use Case

Use case:

```text
Analyze order volume by customer and month, then compare performance under different executor configurations and storage formats.
```

Business questions:

1. Which customers placed the most orders?
2. Which months have the highest order volume?
3. Which order statuses dominate?
4. How does Parquet compare with CSV?
5. How does executor sizing affect runtime?

## 4. Base PySpark Program

Save as:

```text
performance_storage_assignment.py
```

Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, date_format, col
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .appName("PerformanceStorageAssignment") \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")

print("Initial partitions:", orders_df.rdd.getNumPartitions())
print("Total records:", orders_df.count())

status_df = orders_df.groupBy("order_status").count()
status_df.show()

monthly_df = orders_df \
    .withColumn("order_month", date_format(col("order_date"), "MMMM")) \
    .groupBy("customer_id", "order_month") \
    .agg(count("*").alias("order_count"))

monthly_df.orderBy(col("order_count").desc()).show(20)

monthly_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/assignment_results/monthly_orders_parquet")

spark.stop()
```

## 5. Execute With `spark-submit`

Run with dynamic allocation disabled:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 4G \
  --conf spark.dynamicAllocation.enabled=false \
  performance_storage_assignment.py
```

Try different configurations:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 4G \
  --conf spark.dynamicAllocation.enabled=false \
  performance_storage_assignment.py
```

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 2G \
  --conf spark.dynamicAllocation.enabled=false \
  performance_storage_assignment.py
```

## 6. Executor Experiment Table

Capture results like this:

| Run | Executors | Cores Per Executor | Executor Memory | Total Cores | Runtime | Observation |
|---|---:|---:|---:|---:|---:|---|
| 1 | 2 | 2 | 4G | 4 | | Baseline |
| 2 | 1 | 4 | 4G | 4 | | Fewer executors |
| 3 | 4 | 2 | 2G | 8 | | More parallelism |

Explain:

- whether runtime improved
- whether tasks spilled
- whether executors had enough memory
- whether cluster resources were idle
- whether executor memory exceeded YARN limits

## 7. Executor Memory Diagram

For each executor:

```text
Executor container
  |
  +-- JVM heap: --executor-memory
  |     |
  |     +-- Reserved memory: about 300 MB
  |     +-- Unified memory: storage + execution
  |     |     +-- Storage: cache, persist
  |     |     +-- Execution: shuffle, sort, join, aggregation
  |     +-- User memory: user objects, RDD/UDF data
  |
  +-- Overhead memory: outside JVM
  +-- Optional off-heap memory
  +-- Optional PySpark worker memory
```

Use formula:

```text
overhead = max(10% of executor memory, 384 MB)
```

## 8. Storage And Execution Memory Scenarios

### 8.1 Storage Extends Into Free Execution Memory

If storage memory is full and execution memory has free space, cached blocks can use part of the free unified memory.

Example:

```text
Storage reserved area = 500 MB
Execution area        = 500 MB
Cached data           = 700 MB
```

If execution is not using all its memory, storage can use free space.

### 8.2 Execution Later Needs More Memory

If later a shuffle, sort, join, or aggregation needs more execution memory, execution can evict cached storage blocks.

Important:

```text
Execution can evict storage.
Storage cannot evict execution.
```

Result:

- some cached data may be removed
- future actions may recompute evicted cached blocks
- active execution gets priority

## 9. Hash Aggregate Vs Sort Aggregate Demo

Create a temp view:

```python
orders_df.createOrReplaceTempView("orders")
```

Query likely to involve sort behavior:

```python
spark.sql("""
SELECT
    customer_id,
    date_format(order_date, 'MMMM') AS order_month,
    count(1) AS total_count
FROM orders
GROUP BY customer_id, order_month
ORDER BY order_month
""").explain(True)
```

Alternative query:

```python
spark.sql("""
SELECT
    customer_id,
    date_format(order_date, 'MMMM') AS order_month,
    count(1) AS total_count,
    first(int(date_format(order_date, 'MM'))) AS month_num
FROM orders
GROUP BY customer_id, order_month
ORDER BY month_num
""").explain(True)
```

Capture:

- whether Spark used HashAggregate or SortAggregate
- runtime
- shuffle size
- task time

## 10. Logical And Physical Plan Optimization Demo

### 10.1 Predicate Pushdown

```python
filtered_df = orders_df.filter("order_id < 200")
filtered_df.explain(True)
```

Look for pushed filters in the physical plan.

### 10.2 Merging Multiple Filters

```python
spark.sql("""
SELECT order_id, order_status
FROM (
    SELECT order_id, customer_id, order_status
    FROM orders
    WHERE order_id < 500
)
WHERE order_id < 200
""").explain(True)
```

Explain how Spark combines filters.

### 10.3 Merging Multiple Projections

```python
projected_df = orders_df \
    .select("order_id", "customer_id", "order_status") \
    .select("order_id", "order_status")

projected_df.explain(True)
```

Explain how unused columns are removed.

## 11. Schema Evolution Demo

Create three files:

```text
orders1.csv:
1,2013-07-25
2,2013-07-25

orders2.csv:
3,2013-07-25,12111
4,2013-07-25,8827

orders3.csv:
5,2013-07-25,11318,COMPLETE
6,2013-07-25,7130,COMPLETE
```

Write first schema:

```python
schema_v1 = "order_id long, order_date date"
df_v1 = spark.read.schema(schema_v1).csv(f"/user/{username}/datasets/orders1.csv")
df_v1.write.mode("overwrite").parquet(f"/user/{username}/schema_demo/parquet")
```

Append second schema:

```python
schema_v2 = "order_id long, order_date date, customer_id long"
df_v2 = spark.read.schema(schema_v2).csv(f"/user/{username}/datasets/orders2.csv")
df_v2.write.mode("append").parquet(f"/user/{username}/schema_demo/parquet")
```

Read with merge:

```python
merged_df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(f"/user/{username}/schema_demo/parquet")

merged_df.printSchema()
merged_df.show()
```

Discuss:

- adding column
- dropping column
- datatype change
- why datatype changes are risky

## 12. File Format Comparison

Write the same data in different formats:

```python
orders_df.write.mode("overwrite").csv(f"/user/{username}/format_demo/csv")
orders_df.write.mode("overwrite").parquet(f"/user/{username}/format_demo/parquet")
orders_df.write.mode("overwrite").orc(f"/user/{username}/format_demo/orc")
```

Check sizes:

```bash
hadoop fs -du -h /user/<username>/format_demo
```

Compare:

- size
- number of files
- read time
- query time
- partition count

## 13. Compression Comparison

```python
orders_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(f"/user/{username}/compression_demo/parquet_snappy")

orders_df.write \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .parquet(f"/user/{username}/compression_demo/parquet_gzip")

orders_df.write \
    .mode("overwrite") \
    .option("compression", "bzip2") \
    .csv(f"/user/{username}/compression_demo/csv_bzip2")
```

Observe:

```bash
hadoop fs -du -h /user/<username>/compression_demo
```

Explain:

- compression ratio
- read speed
- splittability
- number of partitions

## 14. What To Include In Your Submission

Include:

- selected dataset
- business use case
- PySpark code
- `spark-submit` commands
- executor experiment table
- memory distribution diagram
- Spark UI screenshots or observations
- `explain(True)` output observations
- file format size comparison
- compression comparison
- schema evolution explanation

## 15. Interview-Ready Summary

In Spark, performance depends on data layout, memory allocation, execution planning, and file format. Executor memory is divided into reserved, unified, and user memory, while overhead and off-heap memory live outside JVM heap. Aggregations may use Hash Aggregate or Sort Aggregate depending on query shape and data types. Spark transforms SQL/DataFrame code through parsed, analyzed, optimized, and physical plans using Catalyst. For storage, Parquet and ORC are preferred for analytics because they support column pruning, predicate pushdown, compression, and schema evolution. Compression should balance storage savings with processing speed and splittability.

## 16. Quick Revision

- Vary executors, cores, and memory with dynamic allocation disabled.
- Explain heap, overhead, off-heap, storage, execution, and user memory.
- Use `explain(True)` for logical and physical plans.
- Demonstrate predicate pushdown and projection pruning.
- Compare CSV, Parquet, ORC, and Avro conceptually.
- Demonstrate schema evolution using Parquet and `mergeSchema`.
- Compare compression by size, speed, and splittability.
