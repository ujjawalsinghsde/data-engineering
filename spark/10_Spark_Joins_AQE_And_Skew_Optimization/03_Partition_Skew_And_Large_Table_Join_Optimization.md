# Partition Skew And Large Table Join Optimization

## 1. What Is Partition Skew?

Partition skew happens when one partition contains much more data than other partitions.

In Spark, tasks are usually created per partition. If one partition is huge, one task becomes a straggler.

Example:

```text
Partition 1  -> 100 MB
Partition 2  -> 95 MB
Partition 3  -> 102 MB
Partition 4  -> 4 GB
```

The first three tasks may finish quickly, while the fourth task runs for a long time or fails with memory errors.

## 2. Key Skew Example

Suppose `order_status` has 9 unique values, but one key dominates:

```text
COMPLETE          -> 80 percent
CLOSED            -> 7 percent
PENDING_PAYMENT   -> 5 percent
others            -> small
```

During shuffle:

```text
All COMPLETE records -> same shuffle partition
```

That partition becomes huge.

## 3. Symptoms Of Skew

In Spark UI, skew often appears as:

- one or few tasks taking much longer than others
- one task reading much more shuffle data
- high spill to disk for one task
- executor out-of-memory errors
- low CPU utilization near the end of the job
- stages stuck at 99 percent

Common user-facing symptom:

```text
Most tasks finish fast, but one task keeps running.
```

## 4. Why Skew Reduces Parallelism

Spark parallelism works best when partitions have similar sizes.

Without skew:

```text
200 partitions
200 reasonably balanced tasks
```

With skew:

```text
199 small tasks
1 very large task
```

Even if the cluster has many executors, the large partition may be processed by only one task. The rest of the cluster becomes underused.

## 5. Skew In Aggregations

Example:

```python
orders_df.groupBy("order_status").count()
```

If `COMPLETE` dominates, that key lands in one shuffle partition. Spark must aggregate a huge number of records for that key.

## 6. Skew In Joins

Skew is even more painful in joins.

Example:

```text
orders.customer_id = customers.customer_id
```

If one customer has millions of orders, all records for that customer must meet in the same join partition.

This can create:

- huge shuffle partition
- memory pressure
- very slow join task
- output explosion if both sides have duplicate keys

## 7. Salting

Salting is a technique for handling skew by splitting a dominating key into multiple artificial keys.

Original skewed key:

```text
COMPLETE
COMPLETE
COMPLETE
COMPLETE
```

Salted keys:

```text
COMPLETE_0
COMPLETE_1
COMPLETE_2
COMPLETE_3
```

Now the records can spread across multiple partitions.

## 8. Salting For Aggregation

Goal:

```text
Count orders by order_status
```

If `COMPLETE` is skewed, do two-level aggregation.

Step 1: Add salt.

```python
from pyspark.sql.functions import col, concat_ws, floor, rand

salted_df = orders_df.withColumn(
    "salted_status",
    concat_ws("_", col("order_status"), floor(rand() * 10))
)
```

Step 2: Aggregate by salted key.

```python
partial_df = salted_df.groupBy("salted_status", "order_status").count()
```

Step 3: Aggregate again by original key.

```python
final_df = partial_df.groupBy("order_status").sum("count")
```

Concept:

```text
COMPLETE_0 -> 1000
COMPLETE_1 -> 2000
COMPLETE_2 -> 1500

Final:
COMPLETE -> 4500
```

## 9. Salting For Joins

Skewed joins need careful salting because both sides of the join must use compatible salted keys.

If the large side has skewed keys, add random salt to the large side.

For the smaller side, duplicate the skewed key across all salt values.

Concept:

```text
Large table:
customer_id=101, salt=0
customer_id=101, salt=1
customer_id=101, salt=2

Small table:
customer_id=101 duplicated for salt=0,1,2,...
```

Then join on:

```text
customer_id + salt
```

This spreads the join work.

## 10. AQE Skew Handling

Adaptive Query Execution can detect skewed shuffle partitions at runtime and split them.

Enable AQE:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

Check skew-related settings in your Spark environment:

```python
spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
```

AQE can help, but you still need to understand skew. Severe domain-level skew may require modeling changes or salting.

## 11. Optimizing Joins Between Two Large Tables

When both tables are large, broadcast join is usually not possible.

Options:

1. Filter both tables before join.
2. Select only required columns.
3. Use AQE.
4. Tune shuffle partitions.
5. Use bucketing for repeated joins.
6. Consider partitioning for filter-heavy queries.
7. Handle skewed keys separately.

## 12. Bucketing For Repeated Large Joins

Bucketing stores data using a fixed number of buckets based on a hash of a column.

Example:

```python
orders_df.write \
    .format("parquet") \
    .bucketBy(8, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("retail_bucketed.orders")

customers_df.write \
    .format("parquet") \
    .bucketBy(8, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("retail_bucketed.customers")
```

If both tables:

- are bucketed on the join key
- have the same number of buckets
- are read as Spark tables

Spark can reduce or avoid shuffle for joins.

This is often called a Sort Merge Bucket join.

## 13. Partitioning Vs Bucketing

Partitioning creates folders.

```text
order_status=COMPLETE/
order_status=CLOSED/
order_status=PENDING_PAYMENT/
```

Bucketing creates files based on hash buckets.

```text
bucket_00000
bucket_00001
bucket_00002
...
```

Use partitioning when:

- column has low cardinality
- queries filter frequently on that column
- partition pruning will skip folders

Use bucketing when:

- column has high cardinality
- repeated joins happen on that column
- equality lookup or join optimization matters

## 14. Partitioning Plus Bucketing

You can partition first and bucket inside partitions.

Example:

```python
orders_df.write \
    .format("parquet") \
    .partitionBy("order_status") \
    .bucketBy(8, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("retail_optimized.orders")
```

Concept:

```text
order_status=COMPLETE/
    bucket files
order_status=CLOSED/
    bucket files
```

This can help when queries filter by status and then join by customer.

Important:

Bucketing requires saving as a Spark table. Plain path-based writes may not preserve bucket metadata for the optimizer.

## 15. One-Time Cost Vs Repeated Benefit

Bucketing has an upfront cost:

- read raw data
- shuffle by bucket column
- sort if required
- write bucketed table

But for repeated large joins, it can pay off.

Example:

```text
Without bucketing:
30 joins * 10 minutes = 300 minutes

With bucketing:
one-time preparation = 60 minutes
30 optimized joins * 2 minutes = 60 minutes
total = 120 minutes
```

The exact numbers vary, but the reasoning is the key.

## 16. Join Optimization Checklist

Before optimizing, measure:

- input table sizes
- filtered table sizes
- join key cardinality
- duplicate rate on join key
- skewed key distribution
- shuffle read/write size
- task duration spread
- spill metrics

Then choose:

| Situation | Likely Optimization |
|---|---|
| One small table, one large table | Broadcast Hash Join |
| Both large, repeated joins | Bucketing |
| Low-cardinality frequent filter | Partitioning |
| Dominating key | Salting or AQE skew handling |
| Many tiny shuffle partitions | AQE coalescing or tune shuffle partitions |
| Many unnecessary columns | Projection before join |
| Unnecessary records | Filter before join |

## 17. Common Mistakes

1. Partitioning by high-cardinality columns like `customer_id`.
2. Bucketing both tables with different bucket counts.
3. Writing bucketed files without `saveAsTable`.
4. Assuming bucketing helps every query.
5. Ignoring skewed keys in large joins.
6. Trying to broadcast a medium table that does not safely fit memory.
7. Running joins before filtering.

## 18. Production Guidance

For production joins:

- keep join keys clean and consistently typed
- avoid joining string and integer versions of the same key
- deduplicate dimensions when business logic allows
- isolate known skewed keys and process separately if needed
- enable AQE unless there is a strong reason not to
- use Parquet/ORC rather than CSV for large recurring joins
- inspect physical plans and Spark UI

## 19. Interview Questions

### Beginner

1. What is partition skew?
2. Why does skew make Spark jobs slow?
3. What is salting?
4. What is bucketing?
5. What is partition pruning?

### Intermediate

1. How would you identify skew in Spark UI?
2. How does salting solve aggregation skew?
3. How does AQE handle skewed joins?
4. Why must bucket counts match for bucketed joins?
5. What is the difference between partitioning and bucketing?

### Senior

1. A join between two 1 TB tables takes 2 hours. What is your optimization plan?
2. One customer ID dominates 40 percent of the data. How would you join safely?
3. When would you choose salting over AQE?
4. How would you design a lake table layout for repeated joins and frequent filters?
5. How do you calculate whether bucketing is worth the one-time cost?

## 20. Quick Revision

- Skew means partitions are not balanced.
- One huge partition can slow the entire stage.
- Salting splits a hot key into multiple keys.
- AQE can split skewed shuffle partitions.
- Partitioning helps filtering.
- Bucketing helps high-cardinality lookup and repeated joins.
- For two large repeated joins, bucket both tables on the join key with the same bucket count.
