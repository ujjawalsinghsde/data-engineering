# Spark Partitioning, Data Skew & Repartition vs Coalesce
### Senior Data Engineer — Interview & Revision Notes

---

## 1. PARTITIONING

### 1.1 What it is
Partitioning = splitting data into smaller chunks so it can be processed in parallel.
Two **different** meanings exist — don't mix them up (common interview trap, see 1.16):
- **Spark execution partitions** — in-memory chunks of an RDD/DataFrame, each processed by one task.
- **File/table (disk) partitioning** — physical folder structure (e.g. Hive-style `year=2024/month=01/`) used by storage layers like Parquet/Delta.

### 1.2 Why we use it
- Enables **parallelism** — each partition = 1 task on 1 core.
- Enables **partition pruning** on disk (skip reading irrelevant folders).
- Enables **data locality** and controlled shuffle behavior.
- Without partitioning, data would be processed serially or in one giant chunk → no parallel speedup, possible OOM.

### 1.3 How Spark partitions data internally
- On read: Spark splits input files into partitions based on `spark.sql.files.maxPartitionBytes` (default 128MB) and file splittability.
- On shuffle (wide transformations like `groupBy`, `join`, `distinct`): Spark redistributes data using a **partitioner** (Hash or Range) into `spark.sql.shuffle.partitions` (default 200) partitions.
- Each partition is processed by exactly **one task**; tasks run on executor cores in parallel.

### 1.4 Partition count vs partition size
| | Too many partitions | Too few partitions |
|---|---|---|
| Symptom | Many tiny tasks | Few huge tasks |
| Overhead | Task scheduling overhead dominates | Poor parallelism, executors idle |
| Output | Small-file problem | Very large files, memory pressure |

**Rule of thumb:** target partition size ~128MB–200MB, and partition count ≈ total data size / target size, but not less than total cores available (else you waste parallelism).

### 1.5 Hash vs Range partitioning
- **Hash Partitioning** (default for `groupBy`, `join`, `repartition(col)`): `partition = hash(key) % numPartitions`. Fast, but can cause skew if key distribution is uneven.
- **Range Partitioning** (`repartitionByRange`, used in sort-merge scenarios / `orderBy` for writes): splits data into ranges of sorted key values. Better for range-scan queries and sorted output, avoids hash collisions but needs sampling to build ranges (extra cost).

Use **hash** for general shuffles/joins. Use **range** when downstream needs sorted, evenly-sized output (e.g. writing time-ordered files for range queries).

### 1.6 Partitioning by a single column
```python
df.repartition("country")          # Spark execution partitioning (in-memory, shuffle)
df.write.partitionBy("country").parquet("path")   # Disk/table partitioning
```

### 1.7 Partitioning by multiple columns
```python
df.repartition("country", "year")
df.write.partitionBy("year", "month").parquet("path")
```
On disk, this creates nested folders: `year=2024/month=01/...`. Order matters — put **lower cardinality / most-filtered** column first for effective pruning.

### 1.8 Choosing a good partition column
- **High cardinality** (e.g. `user_id`, `transaction_id`): good for spreading data evenly in **execution** partitioning, but terrible for **disk partitioning** — creates millions of tiny folders/files (small-file problem).
- **Medium cardinality** (e.g. `country`, `region`, `store_id` — 50–500 distinct values): usually the sweet spot for `partitionBy` on disk — good pruning, manageable folder count.
- **Low cardinality** (e.g. `gender`, `is_active` — 2–5 values): bad for disk partitioning (few huge partitions, no pruning benefit, defeats purpose); bad for execution partitioning too (few partitions = poor parallelism, possible skew if uneven).

### 1.9 Why blindly choosing high/low cardinality is problematic
- Blind high cardinality on **disk**: explodes into thousands/millions of small files → NameNode/metastore pressure, slow listing, slow reads (small-file problem).
- Blind low cardinality anywhere: creates few, large, possibly skewed partitions → some tasks do 90% of the work (stragglers).
- **Correct approach:** pick partition column based on **query filter patterns** (what will `WHERE` clauses filter on) + cardinality that yields partitions in the 128MB–1GB range each.

### 1.10 Partition pruning
Skipping partitions/files that can't contain relevant data based on a filter.
```python
df.filter("year = 2024 AND month = 1")   # only reads year=2024/month=01 folder
```
Works only when:
- Table is physically partitioned by the filtered column, AND
- Filter is a direct comparison (not wrapped in a function like `UPPER(country)`), AND
- Predicate pushdown is enabled (default true).

Check in Spark UI / explain plan: look for `PartitionFilters` in `df.explain()`.

### 1.11 Shuffle and its relationship with partitioning
- Shuffle = redistributing data across the cluster so records with the same key land on the same partition (needed for `groupBy`, `join`, `distinct`, `repartition`).
- Shuffle is expensive: disk I/O (shuffle write/read) + network transfer + serialization.
- Number of **output** partitions after a shuffle = `spark.sql.shuffle.partitions` (unless AQE coalesces them).
- Bad partitioning (wrong key, too many/few partitions) directly increases shuffle cost.

### 1.12 File/table partitioning vs Spark execution partitions — clear distinction
| Aspect | Execution Partition (RDD/DataFrame) | File/Table Partition (Hive/Delta) |
|---|---|---|
| Where | In-memory, during a Spark job | On disk, physical folder structure |
| Controlled by | `repartition()`, `coalesce()`, `spark.sql.shuffle.partitions` | `partitionBy()` at write time |
| Purpose | Parallelism within a job | Pruning at read time, avoiding full scans |
| Lifetime | Exists only during job execution | Persists until data is rewritten |
| Changing it | Cheap-ish (in-memory shuffle) | Expensive (rewrite entire dataset) |

### 1.13 `repartition()`
```python
df.repartition(200)                     # change partition count only, full shuffle
df.repartition("country")               # hash partition by column, shuffle
df.repartition(100, "country", "year")  # partitions + multiple columns
```
- **Always causes a full shuffle** (hash partitioner by default).
- Can **increase or decrease** partition count.
- Use when: data is skewed and needs redistribution, or you need more parallelism before a heavy operation, or you need co-location of a key before multiple joins.

### 1.14 `coalesce()`
```python
df.coalesce(10)
```
- **Only decreases** partition count (cannot increase).
- **Avoids full shuffle** — merges existing partitions without moving all data across the network (narrow transformation in most cases).
- Limitation: doesn't rebalance data — if input partitions are uneven, output stays uneven (skew persists or worsens because fewer, unevenly-loaded partitions remain).
- Use when: reducing output file count after a filter that shrank data significantly, and skew isn't a concern.

### 1.15 Controlling output file size
- `spark.sql.files.maxPartitionBytes` (default 128MB): max bytes packed into a single partition when **reading** split files.
- Output file count on write ≈ number of Spark partitions at write time × number of distinct partition-column values (if `partitionBy` used).
- To control output file size directly: `coalesce()`/`repartition()` before write, or set `spark.sql.adaptive.coalescePartitions.enabled = true` (AQE auto-merges small shuffle partitions).

### 1.16 Small-file problem
- Many tiny files (<< block size, e.g. KBs) instead of few well-sized files.
- Causes: over-partitioning on write, high-cardinality `partitionBy`, streaming micro-batches writing frequently.
- Impact: slow listing (metastore/NameNode overhead), slow reads (task-per-file overhead), wasted parallelism.
- Fix: `coalesce()` before write, compaction jobs (`OPTIMIZE` in Delta), fewer/larger micro-batches.

### 1.17 Too many vs too few partitions
- **Too many:** task scheduling overhead > actual work per task; small-file problem on write; driver overhead tracking many tasks.
- **Too few:** underutilized cluster (idle executors), large tasks risk OOM/spill, longer job due to no parallelism, possible skew concentration.

### 1.18 How incorrect partitioning worsens performance
- Wrong `partitionBy` column (low cardinality) → few giant partition folders → full scans anyway, no pruning benefit.
- Wrong high-cardinality `partitionBy` → millions of small files → listing + open/close overhead dominates job time.
- Unnecessary `repartition()` before a narrow op → wasted shuffle cost.
- Not repartitioning before a join on skewed key → straggler tasks, job tail latency.

### 1.19 Practical optimization examples
```python
# Before writing a large fact table partitioned by date
df.repartition("order_date").write.partitionBy("order_date").parquet("path")

# Reduce small files after heavy filtering
filtered_df = df.filter("status = 'ACTIVE'")
filtered_df.coalesce(20).write.parquet("path")

# Enable AQE to auto-optimize shuffle partition count
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### 1.20 Identifying problems (Spark UI / explain plan)
- **Stages tab:** look at task count vs task duration distribution — many tiny tasks (ms) = over-partitioned; few huge tasks (mins) = under-partitioned.
- **Summary Metrics:** compare min/median/max task duration — large gap = skew, not just wrong partition count.
- **Shuffle Read/Write size** per task: uneven sizes = skewed partitions.
- `df.explain(True)`: check `PartitionFilters` (pruning happening), `Exchange` nodes (shuffle happening and partition count).
- `df.rdd.getNumPartitions()`: quick check of current partition count.

### 1.21 Real-world scenarios
- Partitioning a multi-TB fact table by `event_date` for daily incremental loads + fast date-range queries.
- Choosing `region` (medium cardinality) over `customer_id` (high cardinality) for a sales table used by regional dashboards.
- Repartitioning by join key before multiple sequential joins to avoid repeated shuffles.

### 1.22 Interview points
- Difference between execution partitions and disk partitions (very commonly asked).
- Why `partitionBy(high_cardinality_column)` is an anti-pattern.
- How partition pruning works and what breaks it (functions on filter columns).
- `spark.sql.shuffle.partitions` default (200) and why it's often wrong for both very small and very large jobs.
- How AQE changes shuffle partition behavior (`coalescePartitions`, dynamic).

### 1.23 Quick revision summary
- Execution partitions = in-memory parallel units; table partitions = disk folder structure.
- Good partition column = filter-aligned + medium cardinality + even distribution.
- Shuffle happens on wide transformations; partition count/key choice directly drives shuffle cost.
- Too many partitions → overhead & small files; too few → poor parallelism & OOM risk.
- Use `coalesce()` to shrink cheaply, `repartition()` to rebalance/shuffle.

---

## 2. DATA SKEW

### 2.1 What it is
Data skew = uneven distribution of data (or keys) across partitions, so some partitions hold significantly more data than others.

### 2.2 How uneven distribution creates uneven partitions
Hash partitioning maps each key to a partition via `hash(key) % N`. If one key (or a few keys) has disproportionately many rows (e.g. `country = 'US'` is 60% of rows), all those rows land in the same partition(s) regardless of N.

### 2.3 Why skew causes slow jobs (stragglers)
- A stage finishes only when its **last** task finishes.
- If 199 tasks take 10s and 1 task takes 20 minutes (because it has 10x the data), the whole stage waits on that one straggler.
- Wastes cluster resources — most executors sit idle waiting.

### 2.4 How to detect/confirm skew (don't assume)
Before blaming skew, verify — slowness can also be from GC pauses, spill, network, or bad joins.
1. Check **Spark UI → Stages** → task duration distribution (min/25th/median/75th/max).
2. Large gap between median and max task duration = skew signal.
3. Check **Shuffle Read Size** per task in the stage — one/few tasks reading far more bytes.
4. Run `df.groupBy("key").count().orderBy(desc("count")).show()` to directly inspect key distribution.
5. Check for **spill** metrics (Spilled Memory/Disk) on the straggler task — confirms it's data-volume driven.

### 2.5 Common causes/scenarios

**Join skew:** joining on a key where one value dominates (e.g. `null` foreign keys, a default/placeholder `customer_id = -1`, a mega-popular `product_id`). That key's partition becomes huge in the shuffle.

**GroupBy/aggregation skew:** `groupBy("category")` where one category is 80% of rows — one reducer task does most of the work.

**Partitioning-related skew:** `repartition(col)` on a low-cardinality or unevenly distributed column reproduces the same imbalance in execution partitions.

**Hot keys:** a small number of keys (e.g. a viral product, a bot user, `NULL`) receive disproportionate traffic/rows — same effect as above but often at extreme ratios (one key = millions of rows vs others = hundreds).

### 2.6 Effect on memory, CPU, shuffle, execution
- **Memory:** the overloaded task/executor may spill to disk or OOM.
- **CPU:** other executors idle while one core grinds through the hot partition.
- **Shuffle:** shuffle read for the skewed partition is far larger — network/disk bottleneck concentrated on one node.
- **Execution time:** total job time ≈ time of the slowest task, not the average.

### 2.7 Techniques to handle skew

**Salting** — add a random suffix to the skewed key to spread it across multiple partitions, then aggregate/join in two steps.
```python
from pyspark.sql import functions as F

salted = df.withColumn("salt", (F.rand() * 10).cast("int"))
salted = salted.withColumn("salted_key", F.concat_ws("_", "key", "salt"))
# join the other side after exploding it across the same salt range, then drop salt & re-aggregate
```
Use when: single/few extreme hot keys in a join or groupBy, and broadcast isn't possible (both sides large).
Don't use when: skew is mild (AQE alone handles it) — salting adds complexity and an extra aggregation step.

**Broadcast join** — send the small side to all executors, avoiding shuffle on the large/skewed side entirely.
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```
Use when: one side fits comfortably in executor memory (rule of thumb: < ~10s of MB, tunable via `spark.sql.autoBroadcastJoinThreshold`).
Don't use when: "small" side isn't actually small — causes broadcast OOM on executors.

**AQE Skew Join Optimization** — Spark 3+ automatically detects skewed partitions at runtime and splits them into smaller sub-partitions during a sort-merge join.
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```
Use when: on Spark 3+, this should be your **first line of defense** — free, automatic, no code changes.
Limitation: only helps sort-merge joins; doesn't fix groupBy skew.

**Repartitioning** — `df.repartition(n, "key")` to increase spread, though it won't fix skew from a single dominant key by itself (that key still hashes to one partition).

**Choosing a better partition/join key** — e.g. composite key (`customer_id + order_id`) instead of a low-cardinality/skewed single key, if business logic allows.

**Pre-aggregation** — aggregate/reduce data volume before the shuffle (e.g. partial aggregation, or `reduceByKey`-style logic) so less data moves during the expensive stage.
```python
partial = df.groupBy("key", "salt").agg(F.sum("amount").alias("partial_sum"))
final = partial.groupBy("key").agg(F.sum("partial_sum").alias("total"))
```

**Filtering/handling extreme keys separately** — split the dominant key(s) out, process separately (often with broadcast or special logic), then union results back.
```python
hot = df.filter(F.col("key") == "HOT_KEY")
rest = df.filter(F.col("key") != "HOT_KEY")
# handle hot separately (e.g. broadcast join), then union
```
Use when: 1–2 known extreme outlier keys dominate the whole job (e.g. `NULL`, test accounts, a viral SKU).

### 2.8 When each technique should/shouldn't be used — quick guide
| Situation | Best technique |
|---|---|
| One side of join is small | Broadcast join |
| Both sides large, few hot keys | Salting or isolate-and-handle-separately |
| Spark 3+, moderate skew | Enable AQE skew join (try first) |
| GroupBy skew, reducible before shuffle | Pre-aggregation |
| Skew from bad column choice in repartition | Choose different/composite key |

### 2.9 Trade-offs and limitations
- Salting adds complexity, extra shuffle for re-aggregation, and requires careful handling of the "other side" of a join (must be exploded to match salts).
- Broadcast join fails/OOMs if size estimate is wrong — always verify actual size, don't assume.
- AQE skew join only fires above configured skew thresholds (`skewedPartitionFactor`, `skewedPartitionThresholdInBytes`) — won't help mild imbalance.
- Isolating hot keys adds pipeline complexity (union logic, must not double-count/drop rows).

---

## 3. REPARTITION vs COALESCE

| Aspect | `repartition()` | `coalesce()` |
|---|---|---|
| **Shuffle** | Always full shuffle | No full shuffle (merges adjacent partitions) |
| **Partition count** | Can increase or decrease | Can only decrease |
| **Data balance** | Rebalances evenly (hash-based) | Does NOT rebalance — just merges, can stay/become uneven |
| **Performance** | Expensive (network + disk I/O for shuffle) | Cheap (mostly local merge) |
| **Use case** | Fixing skew, increasing parallelism, repartitioning by key before joins | Reducing output files cheaply after filtering, when data is already balanced |
| **Column-based** | Supports `repartition(col)` for hash partitioning by key | No key-based variant — purely reduces count |

### Examples
```python
# Rebalance skewed data across 50 partitions (full shuffle, evens things out)
df_balanced = df.repartition(50, "customer_id")

# Cheaply reduce to 10 output files after a filter shrank the data (no shuffle)
df_small = df.filter("status = 'ACTIVE'").coalesce(10)
```

### Common interview traps
- **"coalesce is always better because it's cheaper"** — false. If data is skewed, `coalesce` just merges unevenly-loaded partitions into fewer unevenly-loaded ones — skew gets worse, not better.
- **"coalesce(1000) on a 100-partition DataFrame will give 1000 partitions"** — false, `coalesce` can only decrease; use `repartition` to increase.
- **"repartition and coalesce always trigger a shuffle"** — false, `coalesce` avoids a full shuffle (narrow dependency in most cases); `repartition` always shuffles.
- Interviewers often ask: *"You have 1000 tiny output files, how do you fix it, and would you use repartition or coalesce?"* → Answer: `coalesce()`, because you're only reducing count and don't need rebalancing (unless the source was skewed, in which case `repartition` to actually redistribute).

---

## SENIOR-LEVEL CHECKLIST
- [ ] Can explain execution vs disk partitioning without conflating them.
- [ ] Can justify a partition column choice using cardinality + query pattern reasoning.
- [ ] Knows how to read Spark UI stage/task metrics to confirm skew (not just guess).
- [ ] Can pick the right skew-handling technique per scenario (broadcast vs salting vs AQE vs isolate-hot-key).
- [ ] Knows `repartition` vs `coalesce` shuffle behavior cold.
- [ ] Can tune `spark.sql.shuffle.partitions` and `maxPartitionBytes` for a given data volume.
- [ ] Understands AQE's role (`coalescePartitions`, `skewJoin`) and enables it by default in Spark 3+.
- [ ] Can diagnose and fix the small-file problem end-to-end.

## COMMON INTERVIEW QUESTIONS
1. What's the difference between a Spark partition and a Hive/table partition?
2. How would you detect data skew in a production job — walk through Spark UI.
3. When would you use salting vs broadcast join vs AQE skew join?
4. Why is `partitionBy(high_cardinality_col)` a bad idea?
5. Difference between `repartition()` and `coalesce()` — internal shuffle behavior?
6. How does `spark.sql.shuffle.partitions` affect performance, and how would you tune it?
7. What causes the small-file problem and how do you fix an existing table that has it?
8. Explain partition pruning and what can silently disable it.
9. Your job has 199 fast tasks and 1 task taking 20x longer — diagnose and fix.
10. How does AQE change partitioning behavior compared to static Spark configuration?

## KEY POINTS TO REMEMBER
1. Execution partitions (memory) ≠ table partitions (disk) — always clarify which one is being discussed.
2. Good partition column = aligns with query filters + medium/even cardinality, not just "high cardinality is always better."
3. Shuffle is triggered by wide transformations; partition key/count choice directly controls shuffle cost.
4. Confirm skew with Spark UI metrics before applying a fix — don't guess.
5. AQE (`skewJoin`, `coalescePartitions`) should be enabled by default and tried first in Spark 3+.
6. Broadcast join eliminates shuffle entirely for small-side joins — biggest lever when applicable.
7. `coalesce()` = cheap, no shuffle, decrease-only, doesn't rebalance.
8. `repartition()` = full shuffle, increase or decrease, rebalances.
9. Small-file problem usually stems from over-partitioning on write or high-cardinality `partitionBy`.
10. Target partition size ~128MB–200MB as a practical rule of thumb for both read and write.