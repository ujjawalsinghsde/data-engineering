# Spark Performance Optimization

## Introduction

Spark performance is mostly about understanding data movement.

The key question:

```text
Is data staying within the same partition, or moving across the cluster?
```

If data moves across cluster, it is a shuffle.

Shuffle is expensive.

This note covers:

- Partitions.
- `defaultParallelism`.
- `defaultMinPartitions`.
- Narrow vs wide transformations.
- `reduceByKey` vs `groupByKey`.
- Jobs, stages, tasks.
- Spark History Server.
- `repartition` vs `coalesce`.
- Cache and persist.

## Partitions

Partition is a chunk of an RDD.

Spark processes data partition by partition.

```text
RDD
  |
  +-- Partition 1
  +-- Partition 2
  +-- Partition 3
```

Each partition is processed by one task.

```text
1 partition -> 1 task
```

## HDFS Blocks And RDD Partitions

Simple rule:

```text
Number of RDD partitions is often close to number of HDFS blocks
```

Example:

```text
1 GB file = 1024 MB
HDFS block size = 128 MB

1024 / 128 = 8 blocks
RDD partitions approx = 8
```

Check partitions:

```python
orders_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/orders/*")
orders_rdd.getNumPartitions()
```

## `defaultParallelism`

`defaultParallelism` tells default number of tasks Spark may use for operations like `parallelize`.

```python
spark.sparkContext.defaultParallelism
```

Example:

```python
words = ("big", "data", "spark")
words_rdd = spark.sparkContext.parallelize(words)
words_rdd.getNumPartitions()
```

If default parallelism is 2, `parallelize` may create 2 partitions unless specified.

## `defaultMinPartitions`

`defaultMinPartitions` is the default minimum partitions used by `textFile`.

```python
spark.sparkContext.defaultMinPartitions
```

Course idea:

```text
100 MB file -> less than 128 MB -> 1 HDFS block
But Spark may still create 2 partitions because defaultMinPartitions = 2
```

For larger files:

```text
300 MB file
128 MB block size
Approx partitions = 3
```

## Narrow Vs Wide Transformations

### Narrow Transformations

No shuffle.

Examples:

- `map`
- `filter`
- `flatMap`

```text
Partition 1 -> map -> Partition 1
Partition 2 -> map -> Partition 2
```

These are usually faster.

### Wide Transformations

Shuffle happens.

Examples:

- `reduceByKey`
- `groupByKey`
- `join`
- `distinct`
- `sortBy`
- `repartition`

```text
Many partitions -> shuffle -> new grouped partitions
```

Wide transformations are expensive because:

- Data moves over network.
- Spark writes shuffle files.
- Sorting/grouping is needed.
- More stages are created.

Best practice:

```text
Filter and map first.
Do wide transformations later.
```

## Jobs, Stages, Tasks

### Job

One action creates one job.

```python
rdd.collect()       # job 1
rdd.count()         # job 2
rdd.saveAsTextFile(...)  # job 3
```

### Stage

Wide transformations split jobs into stages.

Rule:

```text
Number of stages = number of wide transformations + 1
```

Example:

```text
load -> map -> reduceByKey -> collect
```

One wide transformation:

```text
Stages = 2
```

### Task

Task is per-partition work inside a stage.

```text
Number of tasks = number of partitions
```

If stage has 28 partitions, it usually has 28 tasks.

## `reduceByKey` Vs `groupByKey`

This is one of the most important Spark interview topics.

### `reduceByKey`

`reduceByKey` performs local aggregation before shuffle.

Example:

```python
orders_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/orders/orders.csv")

result = (
    orders_rdd
    .map(lambda x: (x.split(",")[3], 1))
    .reduceByKey(lambda x, y: x + y)
)
```

How it works:

```text
Worker 1:
('CLOSED', 1)
('CLOSED', 1)
('PENDING', 1)

Local aggregation:
('CLOSED', 2)
('PENDING', 1)

Only aggregated records are shuffled.
```

Benefits:

- Less shuffle.
- Less network I/O.
- Lower memory pressure.
- Better parallelism.

### `groupByKey`

`groupByKey` groups all values for each key.

Example:

```python
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3], x.split(",")[2]))
grouped_rdd = mapped_rdd.groupByKey()
result = grouped_rdd.map(lambda x: (x[0], len(list(x[1]))))
```

Problem:

`groupByKey` does not aggregate locally. It shuffles all values.

For huge data:

```text
1 TB data
8000 partitions
9 order statuses
```

With `groupByKey`, all records for only 9 statuses may get concentrated on 9 reducer-side partitions.

This can cause:

- Huge shuffle.
- Out of memory.
- Poor parallelism.
- Slow jobs.

### Rule

Use `reduceByKey` when the goal is aggregation.

Use `groupByKey` only when I genuinely need all values for each key, not just aggregate.

## Large Orders Example

Dataset:

```text
/public/ujjawalsingh/orders/orders.csv
Size: around 3.5 GB
Schema: order_id, order_date, customer_id, order_status
```

Blocks:

```text
3500 MB / 128 MB = around 28 blocks
```

RDD partitions:

```text
around 28 partitions
```

Use case:

Count orders by status.

Good:

```python
orders_rdd.map(lambda x: (x.split(",")[3], 1)).reduceByKey(lambda x, y: x + y)
```

Bad:

```python
orders_rdd.map(lambda x: (x.split(",")[3], 1)).groupByKey().mapValues(sum)
```

Why good:

`reduceByKey` aggregates locally before shuffle.

## Repartition

`repartition` changes number of partitions.

It can increase or decrease partitions.

```python
orders_base = spark.sparkContext.textFile("/public/ujjawalsingh/orders/orders_1gb.csv")

more_partitions = orders_base.repartition(15)
less_partitions = orders_base.repartition(4)
```

Important:

`repartition` causes full shuffle.

Use `repartition` when:

- Need to increase partitions.
- Need more parallelism.
- Need more balanced partitions.

Example:

```text
5 GB file
40 partitions
100-node cluster
```

Only 40 tasks can run initially, so 60 nodes may be idle. Repartitioning to more partitions can improve parallelism.

## Coalesce

`coalesce` decreases partitions efficiently.

```python
new_rdd = orders_base.coalesce(5)
```

It cannot increase partitions in normal use.

Example:

```python
orders_base.getNumPartitions()  # 9
orders_base.coalesce(30).getNumPartitions()  # still 9
orders_base.coalesce(5).getNumPartitions()   # 5
```

Use `coalesce` when:

- Data has reduced after filter.
- Many partitions are sparse.
- Need fewer output files.
- Want to avoid full shuffle.

Example:

```text
1 TB file -> 8000 partitions
After filter -> each partition has only 1 MB
```

Coalesce can reduce partition count before writing output.

## Repartition Vs Coalesce

| Point | `repartition` | `coalesce` |
|---|---|---|
| Can increase partitions | Yes | No |
| Can decrease partitions | Yes | Yes |
| Shuffle | Full shuffle | Avoids full shuffle when possible |
| Partition balance | More balanced | May be uneven |
| Best use | Increase partitions | Decrease partitions |

Rule:

```text
Increase partitions -> repartition
Decrease partitions -> coalesce
```

## Cache

Spark transformations are lazy.

If I call two actions on the same uncached RDD, Spark may recompute the full lineage twice.

Example without cache:

```python
orders_base = spark.sparkContext.textFile("/public/ujjawalsingh/orders/orders_1gb.csv")
orders_filtered = orders_base.filter(lambda x: x.split(",")[3] != "PENDING_PAYMENT")
orders_mapped = orders_filtered.map(lambda x: (x.split(",")[2], 1))
orders_reduced = orders_mapped.reduceByKey(lambda x, y: x + y)

orders_reduced.filter(lambda x: int(x[0]) < 501).collect()
orders_reduced.filter(lambda x: int(x[0]) > 10000).collect()
```

Without cache, Spark may recompute `orders_reduced` for both actions.

With cache:

```python
orders_reduced.cache()

orders_reduced.filter(lambda x: int(x[0]) < 501).collect()
orders_reduced.filter(lambda x: int(x[0]) > 10000).collect()
```

First action computes and caches.

Second action reuses cached data.

## Cache Vs Persist

### `cache`

```python
rdd.cache()
```

Default caching is memory-based.

It is a shortcut for a default storage level.

### `persist`

`persist` gives storage options.

```python
from pyspark import StorageLevel

rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.persist(StorageLevel.DISK_ONLY)
```

Use `persist` when:

- RDD does not fit in memory.
- Want memory and disk fallback.
- Need control over storage level.

## When To Cache

Cache when:

- Same RDD is reused multiple times.
- RDD is expensive to compute.
- Interactive analysis needs repeated actions.
- Iterative algorithms are used.

Do not cache when:

- RDD is used only once.
- RDD is very cheap to recompute.
- Cluster memory is limited.
- Data is too large and causes eviction/spill.

## Spark History Server

Course URL:

```text
http://m02.itversity.com:18080/
```

Jobs may appear after notebook/kernel is stopped.

Check:

- Jobs.
- Stages.
- Tasks.
- Shuffle read/write.
- Duration.
- Failed tasks.
- Input/output size.
- Data skew.

## Performance Checklist

When Spark job is slow, check:

- Is there unnecessary `groupByKey`?
- Is shuffle size high?
- Are joins causing large shuffle?
- Are partitions too few?
- Are partitions too many?
- Is data skewed?
- Are we using `collect()` on large data?
- Are repeated actions recomputing same lineage?
- Can cache help?
- Can broadcast join help?
- Can filter be moved earlier?

## Common Mistakes

- Using `groupByKey` for simple counts.
- Using `repartition` to reduce partitions when `coalesce` is enough.
- Caching everything.
- Not caching reused expensive RDDs.
- Ignoring partition count.
- Using too few partitions and underutilizing cluster.
- Using too many partitions and creating overhead.
- Not checking Spark UI.

## Best Practices

- Prefer `reduceByKey` over `groupByKey`.
- Filter before wide transformations.
- Use `coalesce` after heavy filtering.
- Use `repartition` to increase parallelism.
- Cache only reused expensive RDDs.
- Use Spark History Server for proof, not guesswork.
- Avoid `collect()` on large datasets.
- Watch shuffle read/write metrics.

## Interview Questions

### Beginner Questions

- What is partition in Spark?
- What is `getNumPartitions()`?
- What is `defaultParallelism`?
- What is cache?
- What is persist?

### Intermediate Questions

- What is narrow transformation?
- What is wide transformation?
- Why is shuffle expensive?
- What is the difference between `reduceByKey` and `groupByKey`?
- What is the difference between `repartition` and `coalesce`?

### Senior Data Engineer Questions

- How do you tune partition count?
- How do you diagnose data skew?
- When should you cache?
- How do you reduce shuffle?
- Why can `groupByKey` cause OOM?
- How do jobs, stages, and tasks help debugging?

## Quick Revision

```text
Partition = unit of parallelism
Task = one partition work
Job = action
Stage = split by shuffle

Narrow = no shuffle
Wide = shuffle

reduceByKey = local aggregation before shuffle
groupByKey = shuffles all values

repartition = increase/decrease with shuffle
coalesce = decrease efficiently

cache = reuse RDD without recomputing
persist = cache with storage options
```
