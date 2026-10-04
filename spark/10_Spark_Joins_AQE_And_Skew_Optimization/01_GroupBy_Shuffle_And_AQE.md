# GroupBy, Shuffle Partitions, And Adaptive Query Execution

## 1. Why `groupBy` Is A Wide Transformation

In Spark, a `groupBy`, `distinct`, `join`, `orderBy`, and many aggregations are wide transformations.

A wide transformation means data has to move across executors so that related records can be processed together.

Example:

```python
orders_df.groupBy("order_status").count()
```

If the data is distributed across 9 input partitions, each partition may contain records for statuses like:

```text
Partition 1:
CLOSED, COMPLETE, PENDING_PAYMENT

Partition 2:
CLOSED, COMPLETE, PROCESSING

Partition 3:
CLOSED, CANCELLED, COMPLETE
```

To calculate the global count for each status, Spark must bring the same keys together:

```text
CLOSED records              -> one shuffle partition
COMPLETE records            -> one shuffle partition
PENDING_PAYMENT records     -> one shuffle partition
CANCELLED records           -> one shuffle partition
```

That movement is called shuffle.

## 2. Internal Flow Of `groupBy().count()`

Spark does not simply send every raw row immediately to the reducer side. Higher-level APIs apply local aggregation first.

Flow:

```text
Read input partitions
        |
Local aggregation inside each task
        |
Shuffle by grouping key
        |
Final aggregation
        |
Result
```

For:

```python
orders_df.groupBy("order_status").count()
```

Spark may first produce partial counts:

```text
Partition 1:
(CLOSED, 2000)
(COMPLETE, 3000)
(PENDING_PAYMENT, 1000)

Partition 2:
(CLOSED, 1000)
(COMPLETE, 2000)
(PENDING_PAYMENT, 2000)
```

Then the partial counts are shuffled and merged:

```text
CLOSED -> 2000 + 1000 + ...
COMPLETE -> 3000 + 2000 + ...
PENDING_PAYMENT -> 1000 + 2000 + ...
```

This is more efficient than shuffling every raw row.

## 3. Default Shuffle Partitions

By default, Spark uses:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Common default:

```text
200
```

That means a wide transformation usually creates 200 shuffle partitions unless configured otherwise or optimized by Spark.

Problem:

If `order_status` has only 9 distinct values, at most 9 shuffle partitions may contain useful data. The other partitions can be empty.

```text
200 shuffle partitions
9 useful partitions
191 empty or near-empty partitions
```

Even empty partitions create scheduling overhead because Spark still has to plan and launch tasks.

## 4. Why Too Many Shuffle Partitions Hurt

Too many shuffle partitions can cause:

- unnecessary task scheduling overhead
- many small output files
- longer job startup and coordination time
- extra pressure on the Spark driver and scheduler
- inefficient cluster utilization

Too few shuffle partitions can also hurt:

- large partitions
- slower tasks
- out-of-memory errors
- reduced parallelism

The goal is not “more partitions always” or “fewer partitions always”. The goal is balanced partitions.

## 5. Testing Without Writing Output

When testing performance and execution plans, you may not want to write real output files.

Spark supports the `noop` format for testing:

```python
orders_df \
    .groupBy("order_status") \
    .count() \
    .write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

This lets Spark execute the job without creating actual target data.

Use it when:

- testing join strategies
- checking Spark UI DAGs
- validating shuffle behavior
- benchmarking transformations

Do not use it for business pipelines because it discards output.

## 6. Adaptive Query Execution

Adaptive Query Execution, usually called AQE, is a Spark feature that improves query plans at runtime.

Older Spark versions built a plan before execution and followed it even when runtime statistics showed a better choice.

AQE changes this.

During execution, Spark observes runtime statistics such as:

- data size
- number of records
- shuffle partition sizes
- skewed partitions
- relation sizes after filters
- join-side size changes

Then Spark can adjust the physical plan.

## 7. AQE Capabilities

AQE mainly helps in three areas:

1. Dynamically coalescing shuffle partitions
2. Dynamically handling partition skew
3. Dynamically switching join strategies

### 7.1 Coalescing Shuffle Partitions

If Spark creates 200 shuffle partitions but only a few contain data, AQE can combine small partitions into fewer, better-sized partitions.

Without AQE:

```text
200 shuffle partitions
many empty tasks
unnecessary scheduling overhead
```

With AQE:

```text
Spark observes shuffle sizes
small partitions are combined
fewer tasks are launched
```

### 7.2 Handling Partition Skew

If one key dominates the data, one shuffle partition can become much larger than others.

Example:

```text
COMPLETE -> 80 percent of records
CLOSED -> 5 percent
PROCESSING -> 3 percent
others -> small
```

The `COMPLETE` partition may take far longer than all other tasks. AQE can detect skewed partitions and split them.

### 7.3 Switching Join Strategies

Spark may initially plan a sort merge join because a table looks large.

But after filters, one side may become small enough to broadcast.

AQE can switch from:

```text
Sort Merge Join
```

to:

```text
Broadcast Hash Join
```

at runtime.

## 8. AQE Version Notes

General behavior:

- Spark 3.0 introduced AQE, but it often needed to be enabled manually.
- Spark 3.2 and later commonly enable AQE by default in many distributions.

Always check the current environment:

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

Enable it:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

Disable broadcast join while testing AQE shuffle behavior:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

## 9. Useful Configuration Checks

```python
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.get("spark.sql.adaptive.enabled")
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

Set shuffle partitions manually:

```python
spark.conf.set("spark.sql.shuffle.partitions", "50")
```

Use manual tuning carefully. AQE is often better than hardcoding a fixed number for every workload.

## 10. Practical Example

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()

orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")

spark.conf.set("spark.sql.adaptive.enabled", "true")

orders_df \
    .groupBy("order_status") \
    .count() \
    .write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Now check Spark UI:

- number of stages
- shuffle read/write
- number of tasks
- whether AQE coalesced shuffle partitions

## 11. Common Mistakes

1. Assuming 200 shuffle partitions always means 200 useful partitions.
2. Reducing shuffle partitions too much and creating huge tasks.
3. Increasing shuffle partitions blindly and creating scheduler overhead.
4. Ignoring skew because average partition size looks fine.
5. Judging performance from one tiny sample only.
6. Forgetting that filters can change join strategy.

## 12. Production Guidance

Use AQE in production when possible. It helps Spark react to real runtime data sizes.

Still, AQE is not a substitute for good data modeling.

You should still:

- partition data on low-cardinality filter columns
- avoid high-cardinality partitioning
- use bucketing for repeated large joins when appropriate
- avoid unnecessary shuffles
- reduce data early with filters and projections
- inspect Spark UI for skew and shuffle problems

## 13. Interview Questions

### Beginner

1. Why is `groupBy` a wide transformation?
2. What is shuffle in Spark?
3. What is the default number of shuffle partitions?
4. What is local aggregation?
5. Why can some shuffle partitions be empty?

### Intermediate

1. Why can too many shuffle partitions slow down a job?
2. Why can too few shuffle partitions cause memory issues?
3. How does AQE coalesce shuffle partitions?
4. How do you check whether AQE is enabled?
5. What runtime statistics can Spark use during AQE?

### Senior

1. A job has 200 shuffle partitions but only 8 contain data. How would you investigate and optimize it?
2. A `groupBy` job has one task running for 40 minutes while all others finish in 2 minutes. What is likely happening?
3. When would you manually tune `spark.sql.shuffle.partitions` even if AQE is enabled?
4. How would you explain AQE to a non-Spark engineer?
5. What are the limits of AQE?

## 14. Quick Revision

- `groupBy` causes shuffle.
- Spark uses 200 shuffle partitions by default.
- Local aggregation reduces shuffle data.
- Low-cardinality keys can produce many empty shuffle partitions.
- AQE can coalesce shuffle partitions, handle skew, and switch join strategies.
- Always verify using Spark UI, not assumptions.
