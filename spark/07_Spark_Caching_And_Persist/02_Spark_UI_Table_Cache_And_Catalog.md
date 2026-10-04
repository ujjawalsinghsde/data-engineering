# Spark UI, Table Cache, And Catalog

## Introduction

Caching is not just a code concept.

I should also know how to verify caching in Spark UI and how caching behaves for Spark SQL tables.

This topic connects:

- Spark UI
- History Server
- DataFrame cache
- Spark SQL table cache
- `spark.catalog`
- cache invalidation
- `refresh table`

## Spark UI Vs History Server

Spark UI shows details of a running Spark application.

History Server shows details of completed Spark applications.

Simple difference:

| Tool | Shows |
|---|---|
| Spark UI | currently running application |
| History Server | completed applications |

If the application is still running, it may not appear in History Server yet.

Once the application stops:

```python
spark.stop()
```

the completed application details can appear in History Server.

## Why Spark UI Is Useful

Spark UI helps me answer:

- How many jobs ran?
- How many stages were created?
- How many tasks were executed?
- How many executors were used?
- Was data cached?
- How much cached data is in memory/disk?
- Did shuffle happen?
- Did tasks spill to disk?
- Is one stage slower than others?

For caching, the most important tab is:

```text
Storage tab
```

## Spark UI Storage Tab

The Storage tab shows:

- cached RDD/DataFrame/table
- number of cached partitions
- memory used
- disk used
- storage level
- replication

Example text:

```text
Memory Deserialized 1x Replicated
```

Meaning:

- stored in memory
- stored in deserialized form
- one copy exists

## Locality Level

Spark UI may show locality levels like:

```text
NODE_LOCAL
PROCESS_LOCAL
```

## NODE_LOCAL

`NODE_LOCAL` means the task runs on a node where the data block exists locally.

This follows data locality.

Example:

```text
Worker Node 1 has HDFS block B1
Task processing B1 runs on Worker Node 1
```

## PROCESS_LOCAL

`PROCESS_LOCAL` means data is already inside the executor process memory.

This is even better because Spark does not need to read from HDFS or local disk.

Cached data can often lead to `PROCESS_LOCAL` execution.

## Dynamic Allocation

Some clusters use dynamic allocation.

Dynamic allocation means Spark can add or remove executors based on workload.

Example:

```text
initial executors = 2
minimum executors = 2
maximum executors = 10
```

During a bigger job, Spark may request more executors.

When the job becomes idle, executors can be removed.

Important cache note:

If executors are removed, cached blocks on those executors may also disappear unless external shuffle/caching behavior protects them.

In production, dynamic allocation and caching should be considered together.

## Cache And Logical Plans

Spark cache reuse depends on logical plan matching.

Example:

```python
orders_df.select("order_id", "order_status") \
    .filter("order_status = 'CLOSED'") \
    .cache()
```

Then:

```python
orders_df.select("order_id", "order_status") \
    .filter("order_status = 'CLOSED'") \
    .count()
```

This is more likely to reuse cache because the plan matches.

But this may not reuse the same cached plan:

```python
orders_df.filter("order_status = 'CLOSED'") \
    .select("order_id", "order_status") \
    .count()
```

Even though the final result is same, the analyzed logical plan can differ.

Best practice:

```python
cached_closed_orders = (
    orders_df
    .filter("order_status = 'CLOSED'")
    .select("order_id", "order_status")
    .cache()
)

cached_closed_orders.count()
cached_closed_orders.filter("order_id > 100").count()
```

Use the cached variable directly.

## Predicate Pushdown

Predicate means filter condition.

Predicate pushdown means Spark tries to push filters closer to the data source.

Example:

```python
orders_df.filter("order_status = 'CLOSED'").select("order_id", "order_status")
```

This is usually better than selecting too much data first and filtering later.

Why?

Because filtering early reduces data as soon as possible.

Better pattern:

```python
filtered_df = (
    orders_df
    .filter("order_status = 'CLOSED'")
    .select("order_id", "order_status")
    .cache()
)
```

## Caching Spark Tables

Spark SQL tables can also be cached.

Example:

```python
spark.sql("CACHE TABLE retail.orders")
```

Then queries can reuse cached table data:

```python
spark.sql("""
    SELECT order_status, COUNT(*)
    FROM retail.orders
    GROUP BY order_status
""").show()
```

## Eager Vs Lazy Table Cache

For Spark SQL:

```sql
CACHE TABLE table_name
```

is eager by default in many Spark versions.

That means Spark may cache the table immediately.

Lazy table cache:

```python
spark.sql("CACHE LAZY TABLE retail.orders")
```

This marks the table for caching, but data is cached when an action/query uses it.

DataFrame cache is lazy:

```python
orders_df.cache()
```

needs an action to materialize.

## Create Table And Cache Example

```python
username = getpass.getuser()
db_name = f"{username}_caching_demo_db"
table_name = f"{db_name}.orders"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

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

orders_df.write.mode("overwrite").format("csv").saveAsTable(table_name)

spark.sql(f"CACHE TABLE {table_name}")

spark.sql(f"SELECT COUNT(*) FROM {table_name}").show()
```

Why use `username`?

In shared labs, many users may create databases.

User-specific names avoid collision.

## Uncache Spark Table

Uncache a specific table:

```python
spark.sql("UNCACHE TABLE retail.orders")
```

Clear all cache in current session:

```python
spark.sql("CLEAR CACHE")
```

Using catalog API:

```python
spark.catalog.clearCache()
```

Check if table is cached:

```python
spark.catalog.isCached("retail.orders")
```

## `spark.catalog`

`spark.catalog` provides programmatic access to catalog metadata and cache controls.

Useful methods:

```python
spark.catalog.currentDatabase()
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.isCached("table_name")
spark.catalog.cacheTable("table_name")
spark.catalog.uncacheTable("table_name")
spark.catalog.clearCache()
spark.catalog.refreshTable("table_name")
```

## External Table Cache

External table means Spark owns metadata, but data exists at an external location.

Example:

```python
db_name = f"{username}_caching_demo_ext"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_ext (
        order_id long,
        order_date string,
        customer_id long,
        order_status string
    )
    USING csv
    LOCATION '/user/{username}/orders'
""")
```

Cache:

```python
spark.sql(f"CACHE TABLE {db_name}.orders_ext")
```

## Cache Invalidation

Cache invalidation means cached data becomes outdated because underlying data changed.

If data changes through Spark SQL:

```python
spark.sql("""
    INSERT INTO db.orders_ext
    VALUES (111111, '2023-05-29', 222222, 'BOOKED')
""")
```

Spark usually knows the table changed and can invalidate/refresh cache for future use.

But if files are manually added/removed in the backend storage, Spark may not know.

Example:

```text
Someone copies a new file directly into /user/<username>/orders
```

Spark table metadata/cache may still be stale.

Then use:

```python
spark.sql("REFRESH TABLE db.orders_ext")
```

or:

```python
spark.catalog.refreshTable("db.orders_ext")
```

## Why Refresh May Be Needed

There are three layers:

```text
1. Backend files
2. Spark table metadata
3. Cached table/data
```

If backend files change outside Spark, metadata/cache may not automatically update.

`REFRESH TABLE` tells Spark:

```text
forget old file listing/cache information and re-check the table data
```

## Row-Based Vs Column-Based File Formats

Caching is one optimization.

File format is another.

## Row-Based Formats

Examples:

- CSV
- Avro

Data is stored row by row.

If I need only two columns, Spark may still scan more data than needed.

CSV is also expensive because:

- no embedded schema
- parsing text is slower
- compression and column pruning are limited

## Column-Based Formats

Examples:

- Parquet
- ORC

Data is stored column by column.

If I need only:

```text
order_id, order_status
```

Spark can read only those columns.

This is called column pruning.

Parquet works very well with Spark because:

- schema is stored with data
- compression is efficient
- column pruning is supported
- predicate pushdown is supported
- less data may be scanned

## Can Cache Lower Performance?

Yes.

Cache can reduce performance when:

- data is used only once
- cached data is too large
- memory pressure causes spills
- cache evicts useful data
- serialization/deserialization overhead is high
- source format is already efficient and query is simple
- cache becomes stale and refresh overhead appears

Always measure.

Use Spark UI, not guesswork.

## Important Points To Remember

- Spark UI shows running app details.
- History Server shows completed app details.
- Storage tab shows cached data.
- DataFrame cache is lazy.
- SQL `CACHE TABLE` is eager by default unless `CACHE LAZY TABLE` is used.
- Use `UNCACHE TABLE` to remove specific table cache.
- Use `CLEAR CACHE` or `spark.catalog.clearCache()` to clear all cache.
- Use `REFRESH TABLE` when backend files change outside Spark.
- Prefer Parquet/ORC for analytics.

## Common Mistakes

- Expecting running jobs to show in History Server.
- Not checking the Storage tab.
- Caching a DataFrame but using a different logical plan later.
- Forgetting to uncache tables.
- Manually changing backend files and not refreshing table.
- Caching CSV tables instead of converting to Parquet for repeated analytics.
- Hardcoding another user's database/table names.

## Best Practices

- Use user-specific database names in shared labs.
- Assign cached DataFrames to variables.
- Cache filtered/projected data, not raw full data.
- Use `spark.catalog.isCached()` to verify table cache.
- Clear cache after experiments.
- Refresh external tables after backend file changes.
- Use Parquet/ORC for production analytical tables.

## Interview Questions

### Beginner Questions

- What is Spark UI?
- What is History Server?
- Where can you see cached data in Spark UI?
- How do you cache a Spark table?
- How do you uncache a Spark table?

### Intermediate Questions

- What is the difference between `CACHE TABLE` and `CACHE LAZY TABLE`?
- What is `spark.catalog.clearCache()`?
- Why might cached data become stale?
- What does `REFRESH TABLE` do?
- What is predicate pushdown?

### Senior Data Engineer Questions

- How would you debug a stale cached external table?
- How does dynamic allocation affect caching?
- How do file formats affect caching decisions?
- When would you choose table cache instead of DataFrame cache?
- How would you design cache lifecycle management in a shared cluster?

## Scenario-Based Questions

### Scenario 1: Inserted Data Not Showing

An external table was cached. New files were added manually in the backend path, but query results are old.

Fix:

```python
spark.sql("REFRESH TABLE db.table_name")
spark.sql("UNCACHE TABLE db.table_name")
spark.sql("CACHE TABLE db.table_name")
```

Then rerun the query.

### Scenario 2: Cached Query Still Slow

Check:

- Is table actually cached?
- Did query use the cached logical plan?
- How many partitions are cached?
- Is data spilling to disk?
- Is the bottleneck shuffle after cache?
- Is the source already Parquet and fast?

## Real Project Perspective

In production, cache decisions should be explicit and monitored.

Common production pattern:

```text
Read source table
      |
      v
Filter required date range
      |
      v
Select required columns
      |
      v
Cache curated subset
      |
      v
Run multiple business aggregations
      |
      v
Unpersist / clear cache
```

Cache should be treated like resource allocation.

It helps when used with discipline.

It hurts when used everywhere.

## Quick Revision

- Spark UI = running app.
- History Server = completed app.
- Storage tab = cached data.
- `CACHE TABLE` caches table.
- `CACHE LAZY TABLE` caches on first use.
- `UNCACHE TABLE` clears one table.
- `CLEAR CACHE` clears all.
- `REFRESH TABLE` handles backend file changes.
- Cache can hurt performance if misused.
