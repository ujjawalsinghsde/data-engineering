# Cache And Persist Fundamentals

## Introduction

Spark is an in-memory distributed processing engine, but that does not mean every DataFrame automatically stays in memory forever.

When I read a file:

```python
orders_df = spark.read.csv("/path/to/orders")
```

Spark does not permanently keep all rows in memory by default.

Because Spark uses lazy evaluation, it remembers the logical plan:

```text
read file -> parse rows -> create DataFrame
```

Whenever I call an action like `count()`, Spark may execute that plan again.

If the same DataFrame or expensive transformation is reused many times, recalculating it again and again wastes time.

That is where cache and persist help.

## Why Do We Need Cache?

Cache is used to store the result of a DataFrame/RDD/table so Spark can reuse it.

Without cache:

```text
Action 1 -> read from disk -> transform -> result
Action 2 -> read from disk -> transform -> result
Action 3 -> read from disk -> transform -> result
```

With cache:

```text
Action 1 -> read from disk -> transform -> store in cache -> result
Action 2 -> reuse cache -> result
Action 3 -> reuse cache -> result
```

The first action may be slower because Spark has to compute and store data.

Later actions can be faster.

## Simple Analogy

Imagine reading a book from a library shelf.

If I need one paragraph once, I just read it and put the book back.

If I need the same chapter again and again, I keep the book open on my desk.

Cache is like keeping useful data on the desk.

But desk space is limited.

So I should not keep every book open.

## What Can Be Cached?

Spark can cache:

- RDDs
- DataFrames
- Datasets in Scala/Java
- Spark SQL tables
- temporary views

Common PySpark examples:

```python
orders_df.cache()
orders_df.persist()
spark.sql("CACHE TABLE orders")
```

## Cache Is Lazy

This is very important.

When I write:

```python
orders_df.cache()
```

Spark does not immediately cache the data.

It only marks the DataFrame for caching.

The actual cache happens when an action runs:

```python
orders_df.count()
```

Flow:

```text
orders_df.cache()
      |
      v
No action yet, no data cached
      |
      v
orders_df.count()
      |
      v
Spark computes DataFrame and stores partitions in cache
```

Actions that can trigger cache:

- `count()`
- `show()`
- `head()`
- `take()`
- `collect()`
- write operations

## Cache Example

```python
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

orders_df_cached = orders_df.cache()

orders_df_cached.count()
orders_df_cached.filter("order_status = 'CLOSED'").count()
orders_df_cached.groupBy("order_status").count().show()
```

Why this helps:

The same base orders data is reused multiple times.

Spark can avoid rereading the same source data from HDFS for each action.

## When Should I Cache?

Cache when:

- the DataFrame is reused multiple times
- the DataFrame is expensive to compute
- the DataFrame is medium-sized and fits reasonably in memory/disk
- the cached result is used in iterative processing
- the source data is slow to read

Good example:

```python
filtered_orders = (
    orders_df
    .filter("order_date >= '2023-05-01' AND order_date <= '2023-06-30'")
    .select("order_id", "customer_id", "order_status")
    .cache()
)

filtered_orders.count()
filtered_orders.groupBy("order_status").count().show()
filtered_orders.select("customer_id").distinct().count()
```

Here the filtered DataFrame is reused.

## When Should I Not Cache?

Do not cache when:

- the DataFrame is used only once
- the DataFrame is extremely large
- memory is already under pressure
- the cached data changes frequently
- the source is already very fast and computation is simple
- caching causes eviction of more useful cached data

Bad example:

```python
huge_df = spark.read.parquet("/data/very_large_table")
huge_df.cache()
huge_df.count()
```

If this table is huge and used once, caching wastes resources.

## Cache Storage Defaults

Default cache behavior depends on the abstraction.

| Object | Default Cache Storage |
|---|---|
| RDD | memory |
| DataFrame | memory and disk |
| Spark SQL table | memory and disk |

In modern Spark, DataFrame cache generally uses `MEMORY_AND_DISK`.

This means:

- Spark stores partitions in memory when possible.
- If memory is not enough, partitions can spill to local disk.

## HDFS Disk Vs Local Disk

This point is easy to miss.

If data is already on disk in HDFS, why cache to disk again?

Because worker nodes have different disk areas:

```text
Worker Node
    |
    +-- HDFS storage/data blocks
    |
    +-- Local disk for executor/cache/shuffle
```

Original data may be in HDFS.

Cached disk data is stored in executor/local disk area.

Accessing local cached data can be faster than repeatedly reading and recomputing from source.

## Cache With Partitions

Suppose:

```text
File size = 1.1 GB
HDFS block size = 128 MB
Approx blocks = 9
DataFrame partitions = 9
```

When I run:

```python
orders_df.count()
```

Spark may create 9 tasks for the 9 partitions.

If I cache:

```python
orders_df.cache()
orders_df.count()
```

Spark tries to store those partitions in memory/disk.

In Spark UI Storage tab, I may see details like:

```text
Memory Deserialized 1x Replicated
```

This means:

- cached in memory
- stored in deserialized object form
- one copy is stored

## Serialized Vs Deserialized

Serialized means data is stored in binary format.

Pros:

- takes less space
- better for memory saving

Cons:

- needs CPU to convert back before processing

Deserialized means data is stored as objects.

Pros:

- faster to process
- less CPU conversion

Cons:

- uses more memory

Simple comparison:

| Format | Space | CPU Cost | Speed |
|---|---|---|---|
| Serialized | less | more | slower to compute |
| Deserialized | more | less | faster to compute |

On disk, data is generally serialized.

In memory, Spark can store data serialized or deserialized depending on storage level.

## Replication

Storage level can include replication.

Example:

```text
1x replicated
2x replicated
```

If cached data is replicated twice, Spark keeps two copies on different executors/nodes.

This gives better fault tolerance but uses more storage.

Usually `1x` is enough unless recomputation is very expensive and the cluster is unstable.

## Cache And Wide Transformations

Wide transformations create shuffle.

Examples:

- `distinct`
- `groupBy`
- `join`
- `orderBy`

Example:

```python
orders_df.select("order_status").distinct().count()
```

If input has 9 partitions, Spark may do:

```text
9 map-side tasks -> shuffle -> 200 reduce-side tasks -> final aggregation
```

Why 200?

Spark SQL default shuffle partitions:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Default is commonly:

```text
200
```

This means a wide transformation may create 200 shuffle partitions unless configured differently.

## Cache And Smart Partition Use

Spark does not always cache the entire DataFrame immediately.

Example:

```python
orders_df.cache()
orders_df.head()
```

`head()` only needs a small number of rows.

Spark may compute and cache only the partition(s) needed for that action.

If I want to materialize all partitions:

```python
orders_df.cache()
orders_df.count()
```

`count()` touches all partitions.

## `unpersist`

Cached data uses memory/disk.

When I no longer need it, I should uncache it.

```python
orders_df.unpersist()
```

This removes cached blocks.

Good habit:

```python
cached_df = expensive_df.cache()
cached_df.count()

# reuse cached_df many times

cached_df.unpersist()
```

## Cache API Pattern

Recommended:

```python
cached_df = (
    orders_df
    .filter("order_status = 'CLOSED'")
    .select("order_id", "customer_id", "order_status")
    .cache()
)

cached_df.count()
```

Why assign to a variable?

Because Spark cache matching depends on the logical plan.

Using the cached variable avoids confusion.

## Important Points To Remember

- Cache is used when data is reused.
- Cache is lazy for DataFrames/RDDs.
- First action materializes cache.
- Later actions can be faster.
- Do not cache huge one-time-use DataFrames.
- Use `count()` to materialize all partitions.
- Use `unpersist()` to free resources.
- DataFrame cache defaults to memory and disk.
- RDD cache defaults to memory.

## Common Mistakes

- Caching everything.
- Caching very large raw data when only a small filtered subset is needed.
- Calling `cache()` and assuming data is already cached before an action.
- Not assigning cached DataFrame to a variable.
- Forgetting `unpersist()`.
- Using `head()` and thinking all partitions are cached.
- Caching after an action instead of before repeated actions.

## Best Practices

- Cache after filtering/selecting required data.
- Cache DataFrames reused in multiple actions.
- Materialize cache using `count()` if full cache is needed.
- Monitor Spark UI Storage tab.
- Unpersist once done.
- Prefer caching curated intermediate results, not raw massive data.
- Use persist when cache default storage is not suitable.

## Performance Tips

- Filter early before caching.
- Select only required columns before caching.
- Avoid caching high-cardinality wide data unless reused heavily.
- Do not cache data that is cheaper to recompute.
- Tune `spark.sql.shuffle.partitions` for wide transformations.
- Use Parquet/ORC for faster column pruning and better compression.

## Interview Questions

### Beginner Questions

- What is cache in Spark?
- Why is cache used?
- Is cache lazy or eager?
- How do you remove cached data?
- What is the difference between cache and normal DataFrame execution?

### Intermediate Questions

- When should you cache a DataFrame?
- Why should you not cache very large DataFrames?
- What happens when memory is not enough for DataFrame cache?
- What does serialized vs deserialized mean?
- Why does `count()` materialize more cache than `head()`?

### Senior Data Engineer Questions

- How would you decide whether to cache an intermediate DataFrame?
- How can caching reduce performance?
- How do you debug cache usage in Spark UI?
- How does caching interact with shuffle-heavy transformations?
- How do you manage cache lifecycle in a production pipeline?

## Scenario-Based Questions

### Scenario 1: Cache Makes Job Slower

Possible reasons:

- DataFrame used only once.
- Cache is too large.
- Cache causes memory pressure.
- Executors spend time spilling to disk.
- Useful cache is evicted.
- The first action includes cache materialization cost.

Follow-up:

- Check Spark UI Storage tab.
- Compare first and second action times.
- Check executor memory and GC.
- Try caching a smaller filtered DataFrame.

### Scenario 2: Cache Does Not Seem To Work

Possible reasons:

- No action was called after `cache()`.
- Different logical plan is used later.
- Cached DataFrame variable was not reused.
- Data was unpersisted.
- Cache got evicted due to memory pressure.

## Real Project Perspective

In production, cache is useful but dangerous if used casually.

Good use cases:

- same filtered DataFrame used for multiple aggregations
- iterative ML feature processing
- expensive joins reused downstream
- dashboards with repeated queries on same subset

Bad use cases:

- cache every source table
- cache one-time batch output
- cache huge raw table in shared cluster

Production monitoring:

- Storage tab in Spark UI
- executor memory
- cached partition count
- spilled data
- job duration before/after cache
- cluster memory pressure

## Quick Revision

- Cache stores reused data.
- Cache is lazy.
- First action computes and caches.
- `count()` materializes all partitions.
- Use cache for medium, reused data.
- Do not cache everything.
- Use `unpersist()` after use.
- Monitor Storage tab.
