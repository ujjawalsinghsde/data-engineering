# Spark Memory Management

## 1. Why Memory Management Matters

Spark is fast because it performs a lot of work in memory. But memory is also where many production Spark failures begin.

Common symptoms of poor memory planning:

- executor out-of-memory errors
- excessive garbage collection
- shuffle spill to disk
- slow joins and aggregations
- cached data getting evicted
- Python workers consuming unexpected memory
- YARN rejecting the application because requested memory is too high

Understanding executor memory helps you choose better values for:

```text
--executor-memory
--executor-cores
spark.executor.memoryOverhead
spark.memory.fraction
spark.memory.storageFraction
spark.memory.offHeap.enabled
spark.memory.offHeap.size
spark.executor.pyspark.memory
```

## 2. Executor Memory Vs Overhead Memory

When you submit:

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 2G \
  --conf spark.dynamicAllocation.enabled=false \
  prog1.py
```

`--executor-memory 2G` means executor heap memory.

It does not include all memory used by the executor container.

YARN container memory includes:

```text
executor heap memory
+ executor memory overhead
+ optional off-heap memory
+ optional PySpark memory
```

## 3. Memory Overhead

Executor memory overhead is outside the JVM heap.

Common formula:

```text
max(10% of executor memory, 384 MB)
```

For 2 GB executor memory:

```text
10% of 2 GB = about 204 MB
minimum overhead = 384 MB
overhead = 384 MB
```

For 8 GB executor memory:

```text
10% of 8 GB = about 819 MB
overhead = 819 MB
```

If the cluster maximum allocation is 8 GB and you request:

```text
executor memory = 8 GB
overhead = 819 MB
```

YARN may reject the application because total container memory is more than the cluster maximum.

Example error idea:

```text
Required executor memory, overhead, and PySpark memory is above the max threshold.
Check yarn.scheduler.maximum-allocation-mb and yarn.nodemanager.resource.memory-mb.
```

## 4. Executor Heap Memory Layout

Inside executor heap memory, Spark reserves some memory first.

Example:

```text
Executor heap memory = 2 GB
Reserved memory      = 300 MB
Remaining memory     = about 1.7 GB
```

The remaining memory is divided into:

```text
Unified memory area = 60%
User memory         = 40%
```

Default:

```python
spark.conf.get("spark.memory.fraction")
```

Usually:

```text
0.6
```

## 5. Unified Memory

Unified memory is shared by:

- storage memory
- execution memory

Default split:

```python
spark.conf.get("spark.memory.storageFraction")
```

Usually:

```text
0.5
```

That means:

```text
Unified memory = 60% of usable heap
Storage memory = 50% of unified memory
Execution memory = 50% of unified memory
```

Example for 2 GB heap:

```text
2 GB heap
- 300 MB reserved
= about 1.7 GB usable

Unified area = 60% of 1.7 GB = about 1 GB
User memory  = 40% of 1.7 GB = about 700 MB

Storage memory   = about 500 MB
Execution memory = about 500 MB

Overhead memory  = 384 MB outside heap
```

## 6. Memory Area Responsibilities

| Memory Area | Used For |
|---|---|
| Reserved memory | Spark engine internal needs |
| Storage memory | cache, persist, broadcast blocks |
| Execution memory | shuffle, sort, join, aggregation |
| User memory | user-defined data structures, RDD objects, UDF data |
| Overhead memory | VM overhead, native overhead, container overhead |
| Off-heap memory | memory outside JVM heap managed by Spark/Tungsten |
| PySpark memory | Python worker process memory |

## 7. Execution Memory And Storage Memory Borrowing

Storage and execution memory share the unified memory region.

Important rule:

```text
Execution can evict storage memory.
Storage cannot evict execution memory.
```

Why?

Execution memory is needed for active computation. If Spark cannot get execution memory for joins, sorts, or aggregations, the running task can fail or spill heavily.

## 8. Eviction Example

Assume:

```text
Unified memory = 1 GB
Storage region = 500 MB
Execution region = 500 MB
```

Scenario:

```text
Cached data uses 700 MB
Execution now needs 700 MB
```

Spark can evict some cached blocks from storage memory so execution can continue.

But if execution is already using memory, cache cannot forcefully evict execution memory.

## 9. Off-Heap Memory

Off-heap memory is outside JVM heap.

Benefits:

- less pressure on JVM garbage collection
- useful for Tungsten memory management
- can improve performance for certain workloads

Enable:

```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2G")
```

Important:

Off-heap memory is not counted inside `--executor-memory`; the cluster container must still have enough total memory.

## 10. PySpark Memory

PySpark runs Python worker processes in addition to JVM executors.

Python memory may be used by:

- Python UDFs
- Pandas UDFs
- Python libraries
- local Python objects inside tasks

Relevant setting:

```text
spark.executor.pyspark.memory
```

This is more relevant for PySpark than Scala or Java Spark applications.

## 11. Executor Cores And Memory Per Task

Executor memory must be considered together with executor cores.

Example:

```text
Executor memory = 4 GB
Executor cores  = 4
```

If usable execution memory is around 2.4 GB, then each concurrent task roughly has:

```text
2.4 GB / 4 cores = 600 MB per task
```

If each task needs more than that during shuffle or aggregation, you may see spills or failures.

## 12. Tuning Memory Fraction

Default:

```text
spark.memory.fraction = 0.6
spark.memory.storageFraction = 0.5
```

Example:

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 8G \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.memory.fraction=0.8 \
  prog1.py
```

Increasing `spark.memory.fraction` gives more memory to Spark execution/storage and less to user memory.

Do this carefully. If your application uses large user-defined objects, UDFs, or RDD operations, reducing user memory can hurt.

## 13. Practical Spark Session Example

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .config("spark.ui.port", "0") \
    .config("spark.shuffle.useOldFetchProtocol", "true") \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()
```

Example cache workload:

```python
orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")

cached_orders = orders_df.cache()
cached_orders.count()
```

Check Spark UI Storage tab to observe cached size and memory/disk usage.

## 14. Common Mistakes

1. Thinking `--executor-memory` is the total container memory.
2. Forgetting memory overhead.
3. Using too many executor cores and starving each task of memory.
4. Caching large data without enough storage memory.
5. Ignoring Python worker memory in PySpark jobs.
6. Increasing executor memory beyond YARN maximum allocation.
7. Making executors too large and causing long garbage collection pauses.

## 15. Production Guidance

Prefer balanced executors:

- not too thin, or you lose multithreading benefits
- not too fat, or garbage collection and HDFS throughput can suffer
- enough memory per core for shuffle-heavy operations
- enough overhead for PySpark and native memory

For memory-heavy joins and aggregations:

- reduce input columns early
- filter before shuffle
- use broadcast joins when safe
- use AQE
- monitor spill metrics
- avoid unnecessary cache

## 16. Interview Questions

### Beginner

1. What is executor memory?
2. What is memory overhead?
3. What is storage memory used for?
4. What is execution memory used for?
5. What is user memory?

### Intermediate

1. Why can YARN reject an executor memory request?
2. What is unified memory?
3. Can execution memory evict storage memory?
4. Can storage memory evict execution memory?
5. Why does PySpark need extra memory?

### Senior

1. A Spark job spills heavily during joins. What memory areas do you inspect?
2. How do executor cores affect memory per task?
3. When would you configure off-heap memory?
4. How would you size executors for a shuffle-heavy PySpark job?
5. Why can very large executors hurt performance?

## 17. Quick Revision

- `--executor-memory` is JVM heap memory.
- Overhead memory is outside heap.
- Spark reserves about 300 MB for internal engine use.
- Unified memory is shared by storage and execution.
- Execution can evict storage; storage cannot evict execution.
- PySpark may require extra Python worker memory.
- Tune memory together with executor cores.
