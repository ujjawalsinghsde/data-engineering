# Persist And Storage Levels

## Introduction

`cache()` is simple.

`persist()` is flexible.

Both are used to reuse data and avoid recomputation.

But `persist()` lets me choose how Spark stores the data.

Example:

```python
orders_df.cache()
```

is simple.

Example:

```python
from pyspark.storagelevel import StorageLevel

orders_df.persist(StorageLevel.MEMORY_AND_DISK)
```

is more explicit.

## Cache Vs Persist

| Feature | `cache()` | `persist()` |
|---|---|---|
| Purpose | reuse data | reuse data |
| Storage control | default only | custom storage level |
| Simpler | yes | no |
| More flexible | no | yes |
| Use case | normal reuse | when memory/disk/serialization choice matters |

In DataFrames, `cache()` is equivalent to a default persist storage level.

Commonly:

```text
MEMORY_AND_DISK
```

## Persist Syntax

```python
from pyspark.storagelevel import StorageLevel

orders_df.persist(StorageLevel.MEMORY_AND_DISK)
orders_df.count()
```

Just like cache, persist is lazy.

The actual persistence happens when an action runs.

## StorageLevel Arguments

Low-level constructor:

```python
StorageLevel(
    useDisk,
    useMemory,
    useOffHeap,
    deserialized,
    replication
)
```

Meaning:

| Argument | Meaning |
|---|---|
| `useDisk` | store on disk if needed |
| `useMemory` | store in memory |
| `useOffHeap` | use off-heap memory |
| `deserialized` | store as objects instead of serialized bytes |
| `replication` | number of copies |

Example:

```python
StorageLevel(True, False, False, False, 1)
```

Meaning:

```text
use disk = yes
use memory = no
off heap = no
deserialized = no
replication = 1
```

This is disk-only serialized storage.

## Common Storage Levels

## `MEMORY_ONLY`

```python
orders_df.persist(StorageLevel.MEMORY_ONLY)
```

Stores data in memory only.

If not enough memory:

- some partitions may not be cached
- missing partitions will be recomputed when needed

Use when:

- data fits in memory
- recomputation is acceptable
- fastest access is needed

Avoid when:

- data is larger than memory
- recomputation is very expensive

## `MEMORY_ONLY_SER`

```python
orders_df.persist(StorageLevel.MEMORY_ONLY_SER)
```

Stores data in memory in serialized format.

Pros:

- uses less memory

Cons:

- extra CPU needed to deserialize before processing

Use when:

- memory is tight
- CPU overhead is acceptable

Note:

Availability can vary by Spark/PySpark version. In some versions, serialized DataFrame storage behavior is different from RDD storage behavior.

## `MEMORY_AND_DISK`

```python
orders_df.persist(StorageLevel.MEMORY_AND_DISK)
```

Stores data in memory where possible.

Spills remaining partitions to disk.

This is a safe default for many DataFrame workloads.

Use when:

- data may not fully fit in memory
- recomputation is expensive
- disk fallback is acceptable

## `MEMORY_AND_DISK_SER`

```python
orders_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

Stores data serialized in memory and disk.

Pros:

- saves memory
- disk fallback

Cons:

- CPU overhead for serialization/deserialization

Use when:

- data is too large for deserialized memory
- memory pressure is high
- CPU is not the bottleneck

## `DISK_ONLY`

```python
orders_df.persist(StorageLevel.DISK_ONLY)
```

Stores data only on disk.

This is slower than memory but can still avoid recomputation.

Use when:

- data is huge
- memory is limited
- recomputation is very expensive
- local disk is acceptable

Avoid when:

- source read is already cheap
- disk I/O is the bottleneck

## Replicated Storage Levels

Some levels have replication variants, like:

```text
DISK_ONLY_2
MEMORY_ONLY_2
MEMORY_AND_DISK_2
```

Replication means Spark keeps more than one copy.

Example low-level form:

```python
orders_df.persist(StorageLevel(True, False, False, False, 2))
```

This means disk-only with 2 replicas.

Use replication when:

- recomputation cost is very high
- cluster nodes are unstable
- cache loss would be expensive

Cost:

- uses more disk/memory
- more network/data movement

## Off-Heap Memory

Off-heap memory is memory outside the JVM heap.

Spark can use off-heap storage if configured.

Constructor argument:

```python
StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
```

Example:

```text
useOffHeap = True
```

But off-heap needs cluster-level configuration.

Do not randomly enable off-heap without understanding executor memory configuration.

## Executor Memory And Off-Heap

Example worker node:

```text
64 GB RAM
16 CPU cores
```

If the node has:

```text
3 executors
20 GB per executor
5 cores per executor
```

Some memory may be reserved for overhead/off-heap.

Example:

```text
60 GB executor memory
4 GB overhead/off-heap/system usage
```

The exact split depends on Spark/YARN configuration.

Important:

Cache uses executor storage memory.

If cache grows too much, Spark may evict partitions or spill.

## Practical Persist Examples

Read orders:

```python
from pyspark.storagelevel import StorageLevel

orders_schema = """
order_id long,
order_date date,
customer_id long,
order_status string
"""

orders_df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/trendytech/orders/orders_1gb.csv")
)
```

## Disk Only

```python
orders_df.persist(StorageLevel.DISK_ONLY)
orders_df.count()
orders_df.unpersist()
```

Low-level equivalent:

```python
orders_df.persist(StorageLevel(True, False, False, False, 1))
```

## Disk Only With 2 Replicas

```python
orders_df.persist(StorageLevel(True, False, False, False, 2))
orders_df.count()
orders_df.unpersist()
```

## Memory And Disk

```python
orders_df.persist(StorageLevel.MEMORY_AND_DISK)
orders_df.count()
orders_df.unpersist()
```

Low-level equivalent:

```python
orders_df.persist(StorageLevel(True, True, False, True, 1))
```

## Memory And Disk Serialized

```python
orders_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
orders_df.count()
orders_df.unpersist()
```

Low-level equivalent:

```python
orders_df.persist(StorageLevel(True, True, False, False, 1))
```

## Important: Unpersist Before Changing Storage Level

If a DataFrame is already cached/persisted, changing storage level directly can fail or not behave as expected.

Correct pattern:

```python
orders_df.persist(StorageLevel.MEMORY_ONLY)
orders_df.count()

orders_df.unpersist()

orders_df.persist(StorageLevel.DISK_ONLY)
orders_df.count()
```

## Choosing Storage Level

Decision guide:

| Situation | Suggested Storage Level |
|---|---|
| Small/medium data fits memory | `MEMORY_ONLY` |
| Data may not fit memory | `MEMORY_AND_DISK` |
| Memory pressure is high | `MEMORY_AND_DISK_SER` |
| Data huge but recomputation expensive | `DISK_ONLY` |
| Need fault tolerance for cached data | replicated level |

## Cache Vs Persist Performance

There is no one universal winner.

Performance depends on:

- data size
- executor memory
- CPU availability
- serialization overhead
- disk speed
- number of reuses
- cost of recomputation

`MEMORY_ONLY` can be fastest if data fits.

`MEMORY_AND_DISK_SER` can be better when memory is limited.

`DISK_ONLY` may be slower but still better than recomputing complex transformations.

## Measuring Time

In notebooks, use:

```python
import time

start = time.time()
result = df.count()
end = time.time()

print("Time taken:", end - start)
```

For real performance analysis, use Spark UI.

Notebook timing can be noisy because:

- cluster load changes
- dynamic allocation changes executors
- cache warming effects
- first run includes startup overhead

## Important Points To Remember

- `persist()` gives storage-level control.
- `cache()` is simpler.
- Persist is lazy.
- Action is needed to materialize persisted data.
- Use `unpersist()` before applying a different storage level.
- Serialized storage saves memory but uses more CPU.
- Disk-only avoids recomputation but depends on disk I/O.

## Common Mistakes

- Persisting without an action.
- Changing storage level without `unpersist()`.
- Using `MEMORY_ONLY` for data that does not fit memory.
- Using serialized storage without considering CPU overhead.
- Forgetting to unpersist after benchmarking.
- Comparing timings without clearing cache between tests.
- Assuming persist always improves performance.

## Best Practices

- Start with `MEMORY_AND_DISK` for DataFrames unless there is a reason to choose otherwise.
- Benchmark multiple storage levels for critical workloads.
- Use Spark UI Storage tab to compare memory/disk usage.
- Unpersist after each experiment.
- Avoid persistence in shared clusters unless needed.
- Persist after filtering/selecting useful data.

## Interview Questions

### Beginner Questions

- What is `persist()` in Spark?
- What is the difference between `cache()` and `persist()`?
- What is `StorageLevel`?
- How do you remove persisted data?
- Is persist lazy?

### Intermediate Questions

- Explain `MEMORY_ONLY`, `MEMORY_AND_DISK`, and `DISK_ONLY`.
- What is serialized storage?
- Why can serialized storage save memory but increase CPU usage?
- Why should we call `unpersist()` before changing storage level?
- When would you use `DISK_ONLY`?

### Senior Data Engineer Questions

- How would you choose a storage level for a 500 GB intermediate DataFrame?
- How do executor memory settings affect caching and persistence?
- How would you benchmark cache vs persist correctly?
- How can dynamic allocation affect persisted data?
- When would replicated storage levels make sense?

## Scenario-Based Questions

### Scenario 1: `MEMORY_ONLY` Is Slow

Possible reason:

Data does not fit memory. Spark keeps recomputing missing partitions.

Better option:

```python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

### Scenario 2: Serialized Persist Saves Memory But Job Slows

Reason:

Serialization/deserialization uses CPU.

If CPU is already bottleneck, serialized storage can slow processing.

### Scenario 3: Benchmark Results Keep Changing

Possible reasons:

- previous cache not cleared
- executor allocation changed
- cluster was busy
- first run warmed cache
- storage level not changed because DataFrame was already persisted

Fix:

- call `unpersist()`
- clear table cache if needed
- rerun multiple times
- compare Spark UI metrics

## Real Project Perspective

Persist is useful when working with large intermediate DataFrames that are reused.

Example:

```text
Raw Transactions
      |
      v
Filter date range
      |
      v
Join customer/product dimensions
      |
      v
Persist enriched transaction DataFrame
      |
      +--> product revenue report
      +--> customer spending report
      +--> regular customer report
      +--> fraud/risk checks
```

Persisting the enriched dataset can save time because multiple downstream reports reuse the same expensive intermediate result.

## Quick Revision

- `cache()` = simple persistence.
- `persist()` = custom storage level.
- `MEMORY_ONLY` is fastest if data fits.
- `MEMORY_AND_DISK` is safer.
- Serialized storage saves space, costs CPU.
- `DISK_ONLY` avoids recomputation but is slower.
- Always unpersist when done.
