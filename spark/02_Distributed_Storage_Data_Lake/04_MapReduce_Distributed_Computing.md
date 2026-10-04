# MapReduce And Distributed Computing

## Introduction

MapReduce is a programming paradigm for distributed processing.

A programming paradigm means a style or model of writing programs.

MapReduce was created to process huge datasets stored across many machines.

It has two main phases:

```text
Map
Reduce
```

Full internal flow:

```text
Record Reader -> Map -> Shuffle -> Sort -> Reduce
```

As developers, we mainly write:

- Mapper logic.
- Reducer logic.

The framework handles:

- Splitting input.
- Running mappers.
- Shuffling.
- Sorting.
- Running reducers.
- Handling retries.

## Why Distributed Processing Is Needed

Traditional programs usually assume data is on one machine.

But in HDFS, a large file is split into blocks across DataNodes.

Example:

```text
500 MB file
    |
    +-- Block 1: 128 MB on DN1
    +-- Block 2: 128 MB on DN2
    +-- Block 3: 128 MB on DN3
    +-- Block 4: 116 MB on DN4
```

If a traditional program runs on one machine, it would need to pull all data to that machine.

That is slow.

MapReduce runs tasks in parallel near the data.

## Data Locality

Important question:

Does data go to code, or code go to data?

In Big Data, data is huge and code is small. So code goes to data.

```text
Mapper code
    |
    v
Runs near the HDFS block
```

This is called data locality.

Why useful:

- Less network movement.
- Faster processing.
- Better use of cluster resources.

## Mapper Parallelism

Usually, one mapper runs per input split. For a simple understanding, think one mapper per HDFS block.

Example:

```text
500 MB file -> 4 blocks -> 4 mappers
1 GB file   -> 8 blocks -> 8 mappers
```

If the cluster has only 4 available slots, then for a 1 GB file:

```text
8 mappers total
4 run in parallel first
4 run after resources free up
```

Mappers provide parallelism.

## Key-Value Pairs

MapReduce works with key-value pairs.

Format:

```text
(key, value)
```

Examples:

```text
(rollno, studentname)
(1, Satish)
(2, Kapil)
```

For word count:

```text
(word, count)
(hello, 1)
(data, 1)
```

## Record Reader

The record reader converts input data into key-value records for the mapper.

Input line:

```text
hello my name is sumit
```

Record reader output:

```text
(0, "hello my name is sumit")
```

Here:

- Key may be byte offset or line number conceptually.
- Value is the actual line.

The mapper receives this key-value pair.

## Mapper

Mapper processes input records and emits intermediate key-value pairs.

Example input:

```text
(0, "hello my name is sumit")
```

Mapper output:

```text
(hello, 1)
(my, 1)
(name, 1)
(is, 1)
(sumit, 1)
```

Mapper output is intermediate output, not final output.

## Shuffle

Shuffle moves mapper output across the cluster so that all values for the same key reach the same reducer.

Example mapper outputs:

```text
Mapper 1:
(hello, 1)
(sumit, 1)

Mapper 2:
(hello, 1)
(data, 1)

Mapper 3:
(sumit, 1)
(hello, 1)
```

After shuffle, same keys are grouped together:

```text
hello -> 1, 1, 1
sumit -> 1, 1
data  -> 1
```

Shuffle is expensive because it moves data over the network.

## Sort

Before reduce, keys are sorted/grouped.

Example:

```text
(data, [1])
(hello, [1,1,1])
(sumit, [1,1])
```

The framework handles sorting automatically.

## Reducer

Reducer receives each key and list of values.

Reducer input:

```text
(hello, [1,1,1,1,1,1])
(sumit, [1,1])
(is, [1])
```

Reducer output:

```text
(hello, 6)
(sumit, 2)
(is, 1)
```

The reducer creates final aggregated output.

## Word Count Example

Input file:

```text
hello my name is sumit
i love to teach big data
big data is quite interesting
people call me sumit sir
hello this is me
```

Split into blocks:

```text
Block 1:
hello my name is sumit
i love to teach big data

Block 2:
big data is quite interesting

Block 3:
people call me sumit sir

Block 4:
hello this is me
```

Mapper outputs:

```text
(hello, 1)
(my, 1)
(name, 1)
(is, 1)
(sumit, 1)
...
```

Shuffle and sort:

```text
(big, [1,1])
(data, [1,1])
(hello, [1,1])
(is, [1,1])
(sumit, [1,1])
```

Reducer output:

```text
(big, 2)
(data, 2)
(hello, 2)
(is, 2)
(sumit, 2)
```

## Full Flow Diagram

```text
HDFS File
  |
  v
Input Splits / Blocks
  |
  v
Record Reader
  |
  v
Mapper Tasks
  |
  v
Intermediate Key-Value Output
  |
  v
Shuffle
  |
  v
Sort / Group By Key
  |
  v
Reducer Tasks
  |
  v
Final Output In HDFS
```

## MapReduce Vs Spark

MapReduce writes intermediate output to disk heavily.

Spark can keep intermediate data in memory.

That is one big reason Spark is faster for many workloads.

| Point | MapReduce | Spark |
|---|---|---|
| Processing style | Map and reduce jobs | DAG-based transformations/actions |
| Speed | Slower | Faster for many workloads |
| Intermediate data | Often written to disk | Can stay in memory |
| Coding | Java-heavy historically | Python, Scala, SQL, Java, R |
| Use today | Mostly legacy/theory | Common in modern data engineering |

## When MapReduce Is Still Important

Even if we do not write MapReduce code daily, the concept is still important.

Why:

- Spark also performs distributed operations.
- Shuffle is still a major performance bottleneck in Spark.
- GroupBy, joins, aggregations follow similar distributed thinking.
- Interviewers often test MapReduce to check fundamentals.

## Common Mistakes

- Thinking mapper output is final output.
- Forgetting shuffle and sort happen between map and reduce.
- Thinking all data comes to one machine before mapping.
- Ignoring network cost of shuffle.
- Assuming one reducer is always enough.
- Not understanding key-value pair format.

## Performance Tips

- Reduce data as early as possible.
- Avoid unnecessary shuffle.
- Use combiners where possible in classic MapReduce.
- Choose good keys to avoid data skew.
- Use enough reducers, but not too many.
- Compress intermediate output when useful.

## Interview Questions

### Beginner Questions

- What is MapReduce?
- What are the two phases of MapReduce?
- What is a mapper?
- What is a reducer?
- What is a key-value pair?
- What is word count in MapReduce?

### Intermediate Questions

- What is record reader?
- What is shuffle?
- What is sort?
- Why is shuffle expensive?
- How does MapReduce use data locality?
- How many mappers run for a 1 GB file with 128 MB blocks?

### Senior Data Engineer Questions

- How is MapReduce different from Spark?
- How do you reduce shuffle cost?
- What causes reducer skew?
- How would you design a distributed word count job?
- Why is MapReduce considered legacy but still useful to learn?

## Scenario-Based Questions

### Scenario 1: Too Much Shuffle

Your distributed job is slow because shuffle is huge. What do you check?

Answer points:

- Are we grouping or joining too early?
- Can we filter before shuffle?
- Are keys skewed?
- Can we use combiner/pre-aggregation?
- Is partitioning correct?
- Is data format optimized?

### Scenario 2: Word Count Mapper Count

File size is 1 GB. Block size is 128 MB. How many mappers?

Answer:

```text
1 GB = 1024 MB
1024 / 128 = 8
```

Around 8 mappers, assuming one mapper per block/input split for simple explanation.

## Quick Revision

```text
MapReduce = distributed processing paradigm

Flow:
Record Reader -> Map -> Shuffle -> Sort -> Reduce

Mapper = parallel processing
Shuffle = move same keys together
Sort = group/sort keys
Reducer = aggregate final result

Data locality = code goes to data
```
