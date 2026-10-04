# Apache Spark, RDDs, And PySpark

## Introduction

Apache Spark is introduced because MapReduce became a bottleneck.

MapReduce works, but it is:

- Slow for many workloads.
- Hard to code.
- Batch-only.
- Not interactive.
- Too dependent on many ecosystem tools.

Spark solves many of these problems by acting as a general-purpose distributed compute engine.

## Challenges With MapReduce

### 1. Too Much Disk I/O

MapReduce writes intermediate data to disk after every job.

If I chain 5 MapReduce jobs:

```text
MR Job 1 -> read + write
MR Job 2 -> read + write
MR Job 3 -> read + write
MR Job 4 -> read + write
MR Job 5 -> read + write
```

This can become around 10 disk I/O operations.

Disk is much slower than memory.

Spark can keep intermediate data in memory, so it can reduce disk I/O heavily.

### 2. Hard To Write Code

MapReduce usually needs Java and lots of boilerplate.

Even simple logic like word count needs many lines.

### 3. Batch Only

MapReduce is mainly for batch processing.

Modern data systems need:

- Batch processing.
- Streaming.
- SQL.
- Machine learning.
- Interactive notebooks.

### 4. Too Many Tools

Classic Hadoop ecosystem:

```text
Sqoop -> ingestion
Pig   -> scripting/cleaning
Hive  -> SQL
Oozie -> scheduling
MR    -> processing
```

Spark gives many processing capabilities in one engine.

### 5. No Good Interactive Mode

Spark supports shells and notebooks.

This makes development and debugging easier.

## What Is Apache Spark?

Formal definition:

Apache Spark is a multi-language engine for executing data engineering, data science, and machine learning on a single node or cluster.

Simple definition:

```text
Spark = General-purpose + In-memory + Distributed compute engine
```

Meaning:

- General-purpose: Batch, SQL, streaming, ML, graph.
- In-memory: Can store intermediate data in RAM.
- Distributed: Runs work across a cluster.
- Compute engine: It processes data. It does not permanently store data by itself.

## Spark Is Plug And Play

Spark needs two things:

```text
Storage + Resource Manager
```

Storage examples:

- HDFS.
- Amazon S3.
- Azure ADLS Gen2.
- Google Cloud Storage.
- Local storage.

Resource manager examples:

- YARN.
- Kubernetes.
- Mesos.
- Spark Standalone.

Diagram:

```text
HDFS / S3 / ADLS / GCS
        |
        v
Apache Spark
        |
        v
YARN / Kubernetes / Mesos
```

Important interview line:

Spark replaces MapReduce as a processing engine. It does not replace storage like HDFS or resource managers like YARN.

## Spark Languages

Spark supports:

- Scala.
- Python.
- Java.
- R.
- SQL.

Spark with Python is called PySpark.

Python is commonly used because:

- It is easier to learn.
- It has strong data science libraries.
- It works well in notebooks.
- Many data teams already use Python.

## Apache Spark Vs Databricks

Apache Spark is open source.

Databricks is a company and a managed cloud platform built around Spark.

Databricks provides:

- Managed Spark clusters.
- Optimized Spark runtime.
- Cloud integrations.
- Delta Lake.
- Collaborative notebooks.
- Cluster management.
- Jobs/workflows.
- Security features.

Comparison:

| Topic | Apache Spark | Databricks |
|---|---|---|
| What it is | Open-source compute engine | Managed Spark/lakehouse platform |
| Cluster setup | User/team manages | Platform manages |
| Storage | External storage needed | Usually cloud lake + Delta |
| Optimization | Open-source Spark | Optimized runtime |
| Collaboration | Not built-in | Notebook workspace |
| Security | Manually configured | Platform features |

Interview answer:

Spark is the engine. Databricks is a managed platform that runs Spark and adds cloud, security, optimization, notebook, and Delta Lake features.

## Spark APIs

Spark has multiple APIs.

```text
Low level:
    RDD API

Higher level:
    DataFrame API
    Spark SQL

Specialized:
    Structured Streaming
    MLlib
    GraphX
```

Comparison:

| API | Difficulty | Flexibility | Typical Use |
|---|---|---|---|
| RDD | Hardest | Highest | Low-level control |
| DataFrame | Medium | Medium-high | Production ETL |
| Spark SQL | Easiest | Less flexible | SQL transformations |

Important:

RDD is the foundation and helps me understand Spark internals. But in most production projects, DataFrame and Spark SQL are preferred because Spark can optimize them better.

## Basic Spark Job Pattern

Every Spark job usually follows:

```text
Load -> Transform -> Write
```

Example:

```text
Load file from HDFS
    |
    v
Transform with map/filter/reduceByKey
    |
    v
Save result to HDFS
```

## What Is RDD?

RDD stands for Resilient Distributed Dataset.

It is the basic unit of data in Spark Core.

### Resilient

Resilient means fault tolerant.

If a partition is lost, Spark can recreate it using lineage.

Lineage means Spark remembers the chain of transformations used to create an RDD.

### Distributed

Data is split across multiple partitions in the cluster.

```text
RDD
  |
  +-- Partition 1 -> Executor 1
  +-- Partition 2 -> Executor 2
  +-- Partition 3 -> Executor 3
```

### Dataset

RDD is a collection of records.

## RDD Example

Suppose HDFS has a `512 MB` file.

Block size is `128 MB`.

```text
512 MB / 128 MB = 4 blocks
```

When Spark reads it:

```python
rdd1 = spark.sparkContext.textFile("/user/<username>/data/inputfile.txt")
```

Spark creates an RDD with partitions.

Then:

```python
rdd2 = rdd1.map(...)
rdd3 = rdd2.filter(...)
```

Each transformation creates a new RDD.

## RDDs Are Immutable

Immutable means cannot be changed.

When I apply a transformation, Spark does not modify the existing RDD. It creates a new RDD.

Example:

```python
rdd1 = spark.sparkContext.textFile("/path/file.txt")
rdd2 = rdd1.map(lambda line: line.upper())
rdd3 = rdd2.filter(lambda line: "ERROR" in line)
```

Here:

- `rdd1` is original.
- `rdd2` is new.
- `rdd3` is new.

Even if I reuse the variable name:

```python
rdd1 = rdd1.map(lambda line: line.upper())
```

Spark still creates a new RDD internally. I only changed the Python variable reference.

## Transformations And Actions

Spark operations are of two types:

```text
Transformations
Actions
```

### Transformations

Transformations create a new RDD.

They are lazy.

Lazy means they do not execute immediately.

Examples:

- `map`
- `flatMap`
- `filter`
- `reduceByKey`
- `distinct`
- `union`

Example:

```python
rdd2 = rdd1.map(lambda x: x.upper())
```

This only builds the plan.

### Actions

Actions trigger execution.

Examples:

- `collect()`
- `count()`
- `take(10)`
- `first()`
- `saveAsTextFile()`

Example:

```python
rdd2.collect()
```

This actually starts a Spark job.

## Why Transformations Are Lazy

Lazy evaluation helps Spark optimize.

Example:

```python
rdd1 = spark.sparkContext.textFile("/huge/file")
rdd2 = rdd1.map(lambda x: x.upper())
rdd3 = rdd2.filter(lambda x: "ERROR" in x)
rdd3.collect()
```

Spark does not immediately materialize every step.

It builds a plan and executes when an action is called.

Benefits:

- Avoids unnecessary work.
- Allows pipelining.
- Reduces intermediate disk writes.
- Enables optimization.
- Helps failure recovery through lineage.

## DAG

DAG means Directed Acyclic Graph.

It is Spark's execution plan.

```text
rdd1 = load file
    |
    v
rdd2 = flatMap
    |
    v
rdd3 = map
    |
    v
rdd4 = reduceByKey
    |
    v
action = collect/save
```

Spark builds the DAG from transformations.

Action triggers execution of the DAG.

## Driver And Executors

Spark application has:

- Driver.
- Executors.

```text
Driver
  |
  +-- Executor 1
  +-- Executor 2
  +-- Executor 3
```

### Driver

Driver:

- Runs the main program.
- Creates SparkSession/SparkContext.
- Builds DAG.
- Schedules tasks.
- Coordinates executors.
- Receives result for actions like `collect()`.

### Executors

Executors:

- Run tasks.
- Process partitions.
- Store cached data.
- Send status back to driver.

Spark hides much of the distributed complexity. I write transformations, and Spark splits the work across executors.

## SparkSession

SparkSession is the entry point to Spark.

Older Spark had:

- SparkContext.
- SQLContext.
- HiveContext.

SparkSession combines these.

Course boilerplate:

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()
```

Cleaner style:

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession.builder
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

Line explanation:

- `SparkSession.builder`: starts session creation.
- `spark.ui.port = 0`: avoids port conflict.
- `spark.sql.warehouse.dir`: warehouse path in HDFS.
- `enableHiveSupport()`: enables Hive support.
- `master("yarn")`: use YARN as resource manager.
- `getOrCreate()`: create session or reuse existing one.

RDD code uses:

```python
spark.sparkContext
```

DataFrame and SQL code use:

```python
spark
```

## PySpark Word Count

Input:

```text
big data is interesting
big data is trending technology
```

Code:

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/inputfile.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda word: (word, 1))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
rdd4.collect()
```

### Step 1: Load

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/inputfile.txt")
```

Each line becomes one RDD record.

```text
"big data is interesting"
"big data is trending technology"
```

### Step 2: `flatMap`

```python
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
```

`flatMap` splits each line into words and flattens the result.

Output:

```text
big
data
is
interesting
big
data
is
trending
technology
```

Why not `map`?

`map` would create a list per line:

```text
["big", "data", "is", "interesting"]
["big", "data", "is", "trending", "technology"]
```

`flatMap` gives one flat stream of words.

### Step 3: `map`

```python
rdd3 = rdd2.map(lambda word: (word, 1))
```

Output:

```text
(big, 1)
(data, 1)
(is, 1)
```

### Step 4: `reduceByKey`

```python
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)
```

Same keys are aggregated.

Expected output:

```text
(big, 2)
(data, 2)
(is, 2)
(interesting, 1)
(trending, 1)
(technology, 1)
```

### Step 5: Action

```python
rdd4.collect()
```

This triggers execution.

Warning:

`collect()` brings all results to driver. Use it only for small results.

Better for large data:

```python
rdd4.take(10)
rdd4.saveAsTextFile(f"/user/{username}/data/newoutput")
```

## LinkedIn Profile Views In PySpark

Input:

```text
1,Manasa,Sumit
2,Deepa,Sumit
3,Sumit,Manasa
4,Manasa,Deepa
5,Deepa,Manasa
6,Shilpy,Manasa
```

Goal:

Count views for each `to_member`.

Expected:

```text
(Sumit, 2)
(Manasa, 3)
(Deepa, 1)
```

Code:

```python
rdd1 = spark.sparkContext.textFile(f"/user/{username}/data/input/linkedin_views.csv")
rdd2 = rdd1.filter(lambda row: row.strip() != "")
rdd3 = rdd2.map(lambda row: row.split(",")[2])
rdd4 = rdd3.map(lambda name: (name, 1))
rdd5 = rdd4.reduceByKey(lambda x, y: x + y)
rdd5.take(10)
```

Explanation:

- `textFile`: reads CSV rows as strings.
- `filter`: removes empty lines.
- `split(",")[2]`: extracts profile viewed.
- `map(lambda name: (name, 1))`: creates key-value pair.
- `reduceByKey`: counts total views per profile.

Save:

```python
rdd5.saveAsTextFile(f"/user/{username}/data/output")
```

## `reduceByKey` Vs `groupByKey`

For counting, prefer `reduceByKey`.

Better:

```python
rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
```

Worse:

```python
rdd.map(lambda x: (x, 1)).groupByKey().mapValues(sum)
```

Why `reduceByKey` is better:

- It performs local aggregation before shuffle.
- Less data moves over network.
- Similar benefit to combiner in MapReduce.

This is a very important interview point.

## Spark History Server

History server shows completed Spark jobs.

Course URL:

```text
http://m02.itversity.com:18080/
```

Things to check:

- Job duration.
- Stages.
- Tasks.
- DAG.
- Shuffle read/write.
- Failed tasks.
- Executor performance.

Lab note:

Sometimes you need to shut down notebook kernel before completed jobs appear.

## Common Issues

### SparkSession Already Exists

Fix:

```python
spark.stop()
```

Then restart kernel and rerun boilerplate.

### Boilerplate Code Error

Backslash style can fail if there are blank lines.

Safer:

```python
spark = (
    SparkSession.builder
    .config("spark.ui.port", "0")
    .master("yarn")
    .getOrCreate()
)
```

### Output Directory Already Exists

Spark fails if output folder exists.

Fix:

```bash
hadoop fs -rm -R /user/<username>/data/output
```

### Empty Line In CSV

This can cause:

```python
row.split(",")[2]
```

to fail.

Fix:

```python
.filter(lambda row: row.strip() != "")
```

### `collect()` Out Of Memory

Do not collect huge data to driver.

Use:

```python
rdd.take(10)
rdd.saveAsTextFile(...)
```

## Best Practices

- Use `take()` for preview.
- Use `saveAsTextFile()` for large output.
- Avoid `collect()` unless result is tiny.
- Filter empty/bad rows before splitting.
- Use `reduceByKey` instead of `groupByKey` for aggregations.
- Use dynamic username with `getpass.getuser()`.
- Delete or change output path before rerunning.
- Check Spark UI or History Server for job behavior.

## Common Mistakes

- Thinking transformations execute immediately.
- Forgetting actions trigger jobs.
- Using local path instead of HDFS path.
- Saving to existing output path.
- Using `map` where `flatMap` is needed.
- Not handling empty lines in CSV.
- Using `collect()` on large result.
- Confusing Spark with Databricks.

## Interview Questions

### Beginner Questions

- What is Apache Spark?
- What is PySpark?
- What is an RDD?
- What is SparkSession?
- What is transformation?
- What is action?

### Intermediate Questions

- Why are transformations lazy?
- What is DAG?
- Why are RDDs immutable?
- What is lineage?
- Why is `collect()` risky?
- What is the difference between `map` and `flatMap`?
- Why is `reduceByKey` better than `groupByKey`?

### Senior Data Engineer Questions

- When would you use RDD instead of DataFrame?
- How does Spark recover lost partitions?
- How does Spark reduce disk I/O compared to MapReduce?
- How do you debug a slow Spark job?
- What Spark UI metrics do you check first?
- How do you reduce shuffle in Spark?

## Scenario-Based Questions

### Scenario 1: Spark Job Is Slow

What do you check?

- Input file size and partition count.
- Shuffle read/write.
- Skewed keys.
- Executor failures.
- Memory spill.
- Use of `groupByKey`.
- Too many small files.
- Whether `collect()` is used.

### Scenario 2: Driver Crashes

Possible reason:

Large `collect()`.

Fix:

- Use `take()`.
- Write output to HDFS.
- Aggregate more before collecting.

### Scenario 3: LinkedIn Assignment Fails

Possible reason:

Empty line in CSV.

Fix:

```python
rdd.filter(lambda row: row.strip() != "")
```

## Quick Revision

```text
Spark = distributed in-memory compute engine
PySpark = Spark with Python
RDD = Resilient Distributed Dataset

Transformations = lazy
Actions = execute
DAG = execution plan
Driver = coordinator
Executors = workers

Word count:
textFile -> flatMap -> map -> reduceByKey -> action
```
