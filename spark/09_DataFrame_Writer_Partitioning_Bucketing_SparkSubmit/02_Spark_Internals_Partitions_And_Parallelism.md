# Spark Internals, Partitions, And Parallelism

## Introduction

Spark performance depends heavily on:

- number of partitions
- number of available CPU cores
- number of tasks
- shuffle partitions
- file size and splittability
- executor memory

If partitions are too few, resources are wasted.

If partitions are too large, tasks can be slow or fail with memory issues.

If partitions are too many, scheduling overhead and small files become a problem.

## Job, Stage, Task

Spark execution hierarchy:

```text
Application
   |
   v
Job
   |
   v
Stages
   |
   v
Tasks
```

## Job

A job is triggered by an action.

Actions:

- `count()`
- `show()`
- `collect()`
- `take()`
- `write`

Example:

```python
orders_df.count()
```

This creates a Spark job.

## Stage

A stage is a group of tasks that can run without shuffle boundary.

Wide transformations create shuffle boundaries.

Examples of wide transformations:

- `distinct`
- `groupBy`
- `join`
- `orderBy`
- `repartition`

Stages are separated by shuffle.

## Task

A task processes one partition.

If a DataFrame has 9 partitions, a stage usually has 9 tasks.

```text
9 partitions -> 9 tasks
```

## Partitions

A partition is a chunk of distributed data.

Spark processes partitions in parallel.

More partitions can mean more parallelism, but only up to available CPU cores.

Example:

```python
orders_df.rdd.getNumPartitions()
```

## Parallelism

Parallelism means how many tasks can run at the same time.

Simple rule:

```text
maximum parallel tasks ~= total executor cores
```

Example:

```text
4 executors
5 cores each
total cores = 20
```

At most 20 tasks can run in parallel.

If there are 81 partitions:

```text
20 tasks run
20 tasks run
20 tasks run
20 tasks run
1 task runs
```

## Example: 10.1 GB File

Assume:

```text
file size = 10.1 GB
block/partition size ~= 128 MB
partitions ~= 81
resources = 4 executors, 5 cores each
total cores = 20
```

If each task takes around 10 seconds:

```text
first wave: 20 tasks -> 10 sec
second wave: 20 tasks -> 10 sec
third wave: 20 tasks -> 10 sec
fourth wave: 20 tasks -> 10 sec
last wave: 1 task -> around 8 sec
```

Total around:

```text
48 seconds
```

## Example: Too Few Partitions

Assume:

```text
file size = 1.5 GB
partitions = 12
available cores = 20
```

Only 12 tasks can run.

8 CPU cores remain idle.

If each task takes 10 seconds, job finishes in around 10 seconds.

But if repartitioned to 20 partitions:

```text
20 partitions
20 cores
all cores used
smaller partition size
```

Each task may take less time.

Maybe job finishes in 7 seconds.

Lesson:

Too few partitions can underutilize cluster resources.

## Example: Too Many Partitions

Too many partitions can cause:

- task scheduling overhead
- many tiny output files
- too much metadata
- slower job planning

Having 40 partitions on 20 cores is usually okay.

Having 50,000 tiny partitions for small data is not okay.

## Default Parallelism

Check:

```python
spark.sparkContext.defaultParallelism
```

It usually depends on allocated executor cores.

Example:

```text
2 executors
1 core each
defaultParallelism = 2
```

If dynamic allocation is enabled, available resources can change.

## Initial Partitions Vs Shuffle Partitions

These are different.

## Initial Partitions

Initial partitions are created when reading data.

They depend on:

- file size
- file format
- splittability
- `spark.sql.files.maxPartitionBytes`
- `spark.sparkContext.defaultParallelism`
- small file open cost

## Shuffle Partitions

Shuffle partitions are created after wide transformations.

Config:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Default is commonly:

```text
200
```

Example:

```python
orders_df.select("order_status").distinct().count()
```

Possible flow:

```text
Stage 1: 9 tasks
Stage 2: 200 shuffle tasks
Stage 3: 1 final task
```

## How `distinct()` Works

Assume:

```text
orders file = 1.1 GB
initial partitions = 9
```

Stage 1:

Each partition finds local distinct values.

```text
partition 1 -> CLOSED, COMPLETE, PENDING_PAYMENT
partition 2 -> CLOSED, COMPLETE, PROCESSING
...
```

Stage 2:

Spark shuffles same values together.

Example:

```text
partition 23 -> PENDING_PAYMENT values
partition 79 -> COMPLETE values
```

Stage 3:

Final aggregation gives global distinct values.

## CPU Cores Limit Parallel Tasks

If there are 20 cores:

```text
maximum running tasks at a time = 20
```

If stage has 10 tasks:

```text
10 tasks run, 10 cores idle
```

If stage has 60 tasks:

```text
20 tasks run
20 tasks run
20 tasks run
```

## Executor Memory Basics

Executor memory is used for:

- execution memory
- storage memory
- user memory
- reserved memory

Simplified:

```text
Executor Memory
   |
   +-- execution memory: joins, aggregations, shuffle
   +-- storage memory: cache/persist
   +-- user memory: user data structures
   +-- reserved memory: Spark internal reserve
```

Course simplified idea:

- reserved memory around 300 MB
- storage memory and execution memory share significant portions
- execution memory is important for tasks

If each executor has 5 cores and 21 GB, multiple tasks share executor memory.

## Serialized Vs Deserialized Size

Data on disk is serialized.

When loaded into memory, it can become deserialized and take more space.

So a 128 MB partition on disk may need more than 128 MB in memory.

This is why very large partitions can cause memory issues.

## Recommended Partition Size

Common target:

```text
around 128 MB or less per partition
```

This is not a universal law, but a good starting point for many batch workloads.

Too large:

- memory pressure
- slow tasks
- spill
- OOM

Too small:

- too many tasks
- overhead
- small files

## Initial Partitions For Single Splittable File

Spark config:

```python
spark.conf.get("spark.sql.files.maxPartitionBytes")
```

Default commonly:

```text
134217728 bytes = 128 MB
```

For a splittable file:

```text
file size = 1.1 GB
maxPartitionBytes = 128 MB
partitions ~= 9
```

But Spark also considers default parallelism.

Simplified logic:

```text
partition size = min(maxPartitionBytes, fileSize / defaultParallelism)
number of partitions = fileSize / partition size
```

Example:

```text
1.1 GB file
2 cores
min(128 MB, 1.1 GB / 2 = 550 MB) = 128 MB
partitions ~= 9
```

With 16 cores:

```text
min(128 MB, 1.1 GB / 16 = ~75 MB) = 75 MB
partitions ~= 16
```

Spark tries to:

- keep partition size reasonable
- use available cores

## Non-Splittable Files

Some files cannot be split.

Example:

```text
single gzip-compressed CSV file
```

If a 300 MB gzip CSV is not splittable, Spark may create only one partition.

That means one task processes the whole file.

Bad for parallelism.

Important:

- gzip CSV is commonly non-splittable
- some compression with text formats reduces parallelism
- Parquet with compression is splittable because it has internal row groups/blocks

## Multiple Small Files

Small files are a major big data problem.

Spark config:

```python
spark.conf.get("spark.sql.files.openCostInBytes")
```

Default commonly:

```text
4 MB
```

This means Spark treats opening a file as a cost equivalent to reading 4 MB.

If file size is 2 MB:

```text
effective size = 2 MB + 4 MB open cost = 6 MB
```

If max partition size is 128 MB:

```text
128 / 6 ~= 21 small files per partition
```

For 500 small files:

```text
500 / 21 ~= 24 partitions
```

Spark combines small files into partitions to reduce overhead.

## Change `maxPartitionBytes`

Not usually recommended unless there is a clear reason.

Example:

```python
spark = (
    SparkSession.builder
    .config("spark.sql.files.maxPartitionBytes", "146800640")
    .getOrCreate()
)
```

`146800640` bytes = 140 MB.

Changing this affects how Spark creates initial partitions while reading files.

## Assignment Example: Airlines Files

Dataset:

```text
/public/airlines_all/airlines/
```

Read:

```python
airlines_df = (
    spark.read
    .format("csv")
    .load("/public/airlines_all/airlines/")
)

airlines_df.rdd.getNumPartitions()
```

If files are around 64 MB each and open cost is 4 MB:

```text
64 + 4 = 68 MB
```

Two such files:

```text
68 + 68 = 136 MB
```

If max is 128 MB, Spark cannot combine two 64 MB files into one partition.

So partition count remains high.

If max is 140 MB:

```text
136 MB <= 140 MB
```

Spark can combine two files into one partition.

Partition count can reduce.

## Repartition And Coalesce

Change partitions manually:

```python
new_df = df.repartition(50)
```

`repartition`:

- increases or decreases partitions
- causes full shuffle
- creates more balanced partitions

```python
smaller_df = df.coalesce(10)
```

`coalesce`:

- usually decreases partitions
- avoids full shuffle where possible
- may create uneven partitions

Use repartition:

- increase parallelism
- balance data before writing

Use coalesce:

- reduce output file count after filtering

## Common Mistakes

- Confusing initial partitions with shuffle partitions.
- Thinking more partitions always means faster.
- Using too few partitions and wasting CPU cores.
- Using huge partitions and causing OOM.
- Reading one large gzip CSV and expecting parallelism.
- Writing too many small output files.
- Changing `maxPartitionBytes` without measuring.
- Ignoring `openCostInBytes` in small-file scenarios.

## Best Practices

- Check `df.rdd.getNumPartitions()`.
- Check `spark.sparkContext.defaultParallelism`.
- Understand whether source files are splittable.
- Keep partition size reasonable.
- Repartition before writing if output file count matters.
- Avoid many tiny files.
- Prefer Parquet for large analytical datasets.
- Use Spark UI to inspect tasks and stages.

## Interview Questions

### Beginner Questions

- What is a partition in Spark?
- What is a task?
- How are jobs, stages, and tasks related?
- What is default parallelism?
- What is shuffle partition?

### Intermediate Questions

- How does Spark decide initial partitions?
- What is `spark.sql.files.maxPartitionBytes`?
- What is `spark.sql.shuffle.partitions`?
- Why are gzip CSV files bad for parallelism?
- What is small file problem?

### Senior Data Engineer Questions

- How would you tune partitions for a 1 TB dataset?
- How do small files affect Spark performance?
- How do executor cores determine parallelism?
- How would you debug a job with 90% idle CPU cores?
- How do file format and compression affect partitioning?

## Scenario-Based Questions

### Scenario 1: 20 Cores But Only 5 Tasks

Problem:

Only 5 partitions exist.

Fix:

Increase partitions using `repartition`, or read data in a way that creates more partitions.

### Scenario 2: One Task Runs For Very Long

Possible reasons:

- one non-splittable large file
- data skew
- one huge partition
- bad compression format

### Scenario 3: Too Many Tiny Output Files

Fix:

- coalesce/repartition before write
- optimize file size
- compact files later
- avoid over-partitioning by high-cardinality columns

## Quick Revision

- One partition usually maps to one task per stage.
- Parallel tasks are limited by executor cores.
- Initial partitions come from file size, splittability, configs, and default parallelism.
- Shuffle partitions default to 200.
- Wide transformations create shuffle stages.
- Gzip CSV can be non-splittable.
- Small files are combined using open cost logic.
