# Local Checkpoint vs Materialized View vs Temp View

---

## 1. Local Checkpoint (`localCheckpoint()`)

**What it is:** A way to **truncate Spark's lineage (DAG)** by computing a DataFrame *right now* and storing the result on **executor local disk/memory**, instead of Spark remembering "how to recompute it" from the original source.

**Why used:** Spark is lazy — every transformation adds to a lineage (DAG) of "steps to redo if data is lost." After many chained transformations (loops, iterative ML, deeply nested joins/unions), this lineage becomes huge. If a partition is lost, Spark tries to recompute it by replaying the *entire* long chain — slow, sometimes causing stack overflow or driver planning issues. `localCheckpoint()` cuts that chain short: it materializes the current state and gives downstream operations a **fresh, short lineage** starting from this checkpointed data.

**How it works internally:**
1. Spark triggers an action-like computation of the DataFrame right away (eager, not lazy).
2. Results are cached to **executor local storage** (disk/memory of the executors that computed it) — similar to `persist()`.
3. The old lineage is discarded; any new operations on this DataFrame build a new, short lineage starting from the checkpointed data.

**Physically stored or logical reference?** Physically stored (materialized) — but only **locally on executors**, not in a durable/shared filesystem.

**Where stored:** Executor local disk / local temp storage (whatever the executor's local storage is — not HDFS/S3/DBFS).

**Survives executor/cluster failure?** **No.** This is the key weakness. If the executor holding the checkpointed partitions is lost (crash, spot-instance eviction, node failure), that data is **gone** — and because the original lineage was already discarded, Spark **cannot recompute it**. This can cause job failure with no way to recover that partition.

**Effect on lineage:** Breaks/truncates it. This is the entire point — new operations get a short, clean DAG instead of an ever-growing one.

**Performance implications:**
- Pro: Avoids expensive/huge lineage replay, avoids `StackOverflowError` on very deep DAGs (common in iterative algorithms, e.g. GraphX/ML loops).
- Con: The eager computation itself costs time; local disk write/read adds I/O; no fault tolerance safety net.

**When to use:** Iterative algorithms (ML training loops, graph algorithms) where lineage grows every iteration and you don't need full fault tolerance — speed matters more than resilience, and the job can tolerate a rare full restart on failure.

**When NOT to use:** Production critical pipelines on huge datasets where losing an executor mid-job would be costly to redo from scratch; anytime full fault tolerance is required.

**Important pitfalls:**
- No durability — a lost executor can silently kill your job's ability to recover.
- On huge datasets, local disk of executors may not have enough space, causing spills/failures.
- Since lineage is gone, Spark has no fallback plan — this is a deliberate fault-tolerance trade-off, not a bug.

**`localCheckpoint()` vs reliable `checkpoint()`**

| | `localCheckpoint()` | `checkpoint()` |
|---|---|---|
| Storage location | Executor local disk | Reliable distributed storage (HDFS/S3/DBFS) — set via `sparkContext.setCheckpointDir()` |
| Fault tolerant | No | Yes — survives executor loss |
| Speed | Faster (local I/O) | Slower (network + durable storage write) |
| Use case | Iterative jobs, non-critical intermediate state | Production pipelines needing true fault tolerance |

**PySpark example:**
```python
df = spark.read.parquet("s3://bucket/raw_data")
for i in range(20):
    df = df.withColumn(f"col_{i}", df["value"] * i)  # lineage grows every loop

df = df.localCheckpoint()   # lineage truncated here, computed eagerly, stored on executors
df.count()  # triggers the checkpoint computation
```

**Remember this:** *`localCheckpoint()` = fast lineage-cutting trick using executor-local storage — great for speed, zero fault tolerance.*

---

## 2. Materialized View

**What it is:** A database object that **stores the precomputed result** of a query physically on disk, like a real table — but is defined by (and tied to) a query, and can be refreshed later.

**Why used:** To avoid recomputing expensive queries (heavy joins, aggregations) every time someone reads them. Read the stored result instantly instead of re-running the whole query.

**How it works internally (high level):**
1. You define it with a `CREATE MATERIALIZED VIEW ... AS SELECT ...`.
2. The engine runs the query once and **writes the result to physical storage**.
3. On future reads, the engine just reads the stored result — no recomputation, unless a refresh is triggered.
4. Some platforms (Databricks, Snowflake, BigQuery) support **automatic/incremental refresh**, updating only the changed parts instead of recomputing everything.

**Physically stored or logical reference?** **Physically stored** — real data on disk, just like a table.

**Where stored:** In the database/warehouse's managed storage (e.g., Delta table under the hood in Databricks, dedicated storage in Snowflake/BigQuery/PostgreSQL).

**Survives failure?** Yes — it's durable storage, same guarantees as a regular table.

**Effect on lineage:** Not a Spark DAG/lineage concept at all — it's a warehouse/catalog object. Downstream queries reading it start fresh (they read stored data, no re-execution of the original defining query needed).

**Refresh/update behavior:**
- **Manual refresh:** `REFRESH MATERIALIZED VIEW view_name;` — re-runs and overwrites the stored result.
- **Automatic/scheduled refresh:** Many modern platforms (Databricks Delta Live Tables materialized views, Snowflake, BigQuery) refresh automatically, often incrementally (only reprocessing new/changed source data instead of a full recompute).
- Until refreshed, the view can serve **stale** data — an important trade-off to know.

**Performance implications:** Big win for repeated, expensive queries — pay the computation cost once (or incrementally), read many times for near-zero cost. Refresh itself has a cost, and storage is used (unlike a plain view).

**When to use:** Dashboards/BI queries hitting the same expensive aggregation repeatedly; data that doesn't need to be real-time-fresh; reducing load on source tables.

**When NOT to use:** When you need truly real-time/up-to-the-second data and can't tolerate staleness; for cheap, rarely-run queries where materializing isn't worth the storage/refresh overhead.

**Important pitfalls:** Staleness if refresh isn't scheduled/triggered correctly; extra storage cost; refresh can be expensive itself if not incremental; not all SQL engines support them the same way (e.g., plain open-source Spark SQL has no native `CREATE MATERIALIZED VIEW` — this is more a Databricks SQL/Delta Live Tables, Snowflake, BigQuery, PostgreSQL feature).

**SQL example:**
```sql
CREATE MATERIALIZED VIEW sales_summary AS
SELECT region, SUM(amount) AS total_sales
FROM sales
GROUP BY region;

-- Later, refresh manually if not on auto-refresh:
REFRESH MATERIALIZED VIEW sales_summary;

-- Reads are instant — no recomputation:
SELECT * FROM sales_summary;
```

**Remember this:** *Materialized View = a saved, physical snapshot of a query's result — fast to read, but can go stale until refreshed.*

---

## 3. Temporary View / Temp View

**What it is:** A **named, logical reference** to a DataFrame/query — lets you run SQL against a DataFrame using its name. It does **not** store or precompute any data itself.

**Why used:** Convenience — write SQL instead of DataFrame API calls, or let multiple SQL queries refer to the same DataFrame by name within a session/notebook.

**How it works internally:** `createOrReplaceTempView()` just registers the DataFrame's **logical plan** (its lineage/recipe) under a name in the current Spark session's catalog. No computation happens at registration time. Every time you query the temp view, Spark looks up that logical plan and executes it (subject to the usual laziness/optimization) — exactly as if you'd re-run the original DataFrame code.

**Physically stored or logical reference?** **Purely logical** — no data is written or cached anywhere just by creating it.

**Where stored:** Nowhere (no data storage) — only the query/logical-plan definition lives in the Spark session's in-memory catalog.

**Survives failure?** N/A for data (there's no materialized data to lose) — but the view registration itself disappears if the Spark session ends (see lifetime below), since it's session metadata, not durable storage.

**Effect on lineage:** **Does not break lineage at all.** Querying a temp view still walks the full original DataFrame lineage underneath — it's just a name pointing at the same plan. If the underlying DataFrame has 50 chained transformations, querying the temp view re-triggers all 50 (subject to caching if you separately called `.cache()`).

**Lifetime/scope:** Tied to the **SparkSession** that created it — exists only for that session (that notebook/job run). Gone once the session ends. (`createGlobalTempView()` is the variant that persists across sessions on the same cluster, stored in the `global_temp` database, until the cluster/application stops.)

**Performance implications:** **None by itself** — it's just a name, not a cache. A very common interview trap: people assume creating a temp view speeds things up. It doesn't, unless you *also* `.cache()`/`.persist()` the DataFrame.

**When to use:** You want to query a DataFrame using SQL syntax; you need multiple SQL statements/notebook cells to reference the same DataFrame by name.

**When NOT to use:** As a performance optimization — use `.cache()`, `.persist()`, or a materialized view/checkpoint instead if you need to avoid recomputation.

**Important pitfalls:** Assuming it "saves" data (it doesn't); expecting it to survive across sessions (use global temp view or a real table for that); expecting repeated queries to be fast without separately caching.

**PySpark + SQL example:**
```python
df = spark.read.parquet("s3://bucket/sales")
df.createOrReplaceTempView("temp_sales")

# Query it with SQL — re-executes the underlying DataFrame plan each time
spark.sql("SELECT region, SUM(amount) FROM temp_sales GROUP BY region").show()
```

**Remember this:** *Temp View = just a nickname for a DataFrame's query plan — zero data stored, zero performance benefit on its own.*

---

## Comparison Table

| | Local Checkpoint | Checkpoint (reliable) | Temp View | Materialized View |
|---|---|---|---|---|
| Data physically stored? | Yes (local) | Yes (durable) | No (logical only) | Yes (durable) |
| Storage location | Executor local disk | HDFS/S3/DBFS | None — session catalog only | Warehouse/table storage |
| Breaks Spark lineage? | Yes | Yes | No | N/A (not a Spark lineage concept) |
| Survives executor/cluster failure? | No | Yes | N/A (no data to lose) | Yes |
| Improves performance by itself? | Yes (avoids recompute + shortens DAG) | Yes (same, but slower to write) | No | Yes (precomputed reads) |
| Can go stale? | No (recomputed fresh each time you build it) | No | No (always reflects current plan) | Yes, until refreshed |
| Typical scope/lifetime | Job/application | Job/application (until manually cleaned) | Current Spark session | Persistent, until dropped |

---

## Common Interview Questions

1. **Q: What problem does `localCheckpoint()` solve?**
   A: It truncates a long-growing Spark lineage/DAG by eagerly materializing the DataFrame, preventing expensive/slow recomputation and stack overflow risk on very deep DAGs (common in iterative loops).

2. **Q: Is data from `localCheckpoint()` fault tolerant?**
   A: No — it's stored only on executor local disk. If that executor is lost, the data is gone and cannot be recomputed since the original lineage was discarded.

3. **Q: Does creating a temp view cache or store any data?**
   A: No. It only registers the DataFrame's logical plan under a name in the session catalog — querying it re-executes the underlying plan.

4. **Q: How is a materialized view different from a regular/temp view?**
   A: A regular/temp view is just a saved query definition (no stored data, always re-executed). A materialized view actually stores the query's result physically on disk, so reads are fast and don't recompute — but the data can become stale until refreshed.

5. **Q: How do you keep a materialized view's data up to date?**
   A: Either manually via `REFRESH MATERIALIZED VIEW`, or automatically via scheduled/incremental refresh, depending on the platform (Databricks, Snowflake, BigQuery support auto/incremental refresh).

6. **Q: What's the difference between `localCheckpoint()` and `checkpoint()`?**
   A: Both break lineage by eagerly materializing data, but `checkpoint()` writes to a reliable distributed filesystem (fault tolerant), while `localCheckpoint()` writes to executor-local storage (fast, but not fault tolerant).

7. **Q: Does a temp view break Spark's lineage like a checkpoint does?**
   A: No — a temp view is purely a name for the existing plan; the full lineage is still walked every time you query it.

8. **Q: When would `cache()`/`persist()` alone not be enough, requiring a checkpoint instead?**
   A: When the lineage itself is so long/deep that even recomputing from cached-but-evicted data would replay a massive DAG (or risk `StackOverflowError`) — checkpointing physically cuts the DAG, not just caches data.

---

## Scenario-Based Questions

**Q: You have a very long Spark lineage after many iterative transformations. What would you use, and why?**
A: `checkpoint()` (reliable) if this is a production job where fault tolerance matters — it truncates lineage and survives executor loss. `localCheckpoint()` if it's a non-critical iterative job (e.g., ML training loop) where speed matters more and you can tolerate restarting on failure.

**Q: Why doesn't creating a temp view improve performance by itself?**
A: Because it doesn't store or precompute any data — it's just a named pointer to the DataFrame's existing logical plan. Every query against it re-executes that plan from scratch, same cost as querying the DataFrame directly. Performance only improves if you separately `.cache()`/`.persist()` the DataFrame or materialize it.

**Q: Why can `localCheckpoint()` be risky for huge production datasets?**
A: Because the checkpointed data lives only on executor local disk with no durability. On a big dataset, (a) local disks may not have enough space, and (b) if any executor holding checkpointed partitions is lost (common with spot instances or node failures), that data can't be recovered — the original lineage needed to recompute it was already discarded. This can crash the job with no recovery path.

**Q: When would you choose a materialized view over a normal view or temp view?**
A: When the same expensive query (heavy joins/aggregations) is read repeatedly, especially by dashboards/BI tools, and slight data staleness is acceptable — you pay the computation cost once (or incrementally on refresh) instead of on every read. A normal/temp view would re-run the full expensive query every single time it's queried.