# MapReduce Deep Dive

## Introduction

MapReduce is a distributed processing model used to process huge amounts of data.

Earlier, I learned the basic flow:

```text
Record Reader -> Mapper -> Shuffle -> Sort -> Reducer
```

Now the focus becomes deeper:

- How mapper gives parallelism.
- How reducer performs aggregation.
- How many mappers and reducers are launched.
- Why reducer count matters.
- What partitioning means.
- What shuffle and sort actually do.
- Why combiner improves performance.
- When combiner is dangerous.
- How MapReduce ideas connect to Spark.

The main rule I should remember:

```text
Mapper = parallel processing
Reducer = aggregation
```

Performance rule:

Try to do more work at mapper side and minimum work at reducer side.

Why?

Because mappers run in parallel on different blocks. Reducers receive data from many mappers and can become bottlenecks.

## Why MapReduce Is Needed

Traditional programs usually assume data is available on one machine.

But in HDFS, a large file is split into blocks across DataNodes.

Example:

```text
1 GB file
Block size = 128 MB

Number of blocks = 1024 / 128 = 8 blocks
```

These blocks may be stored across different DataNodes.

Instead of bringing all data to one machine, MapReduce sends processing logic near the data.

This is called data locality.

```text
Data is large
Code is small

So code goes to data
```

## Full MapReduce Workflow

The complete flow is:

```text
Input File
    |
    v
Record Reader
    |
    v
Mapper
    |
    v
Combiner (optional)
    |
    v
Partitioner
    |
    v
Shuffle
    |
    v
Sort
    |
    v
Reducer
    |
    v
Output Files
```

What I write as a developer:

- Mapper logic.
- Reducer logic.
- Optional combiner logic.
- Optional custom partitioner logic.

What framework handles:

- Splitting input.
- Starting map tasks.
- Moving data during shuffle.
- Sorting keys.
- Calling reducer.
- Writing output.
- Retrying failed tasks.

## Record Reader

Record Reader converts raw input into key-value pairs.

Example line:

```text
hello my name is sumit
```

Record Reader output:

```text
(0, "hello my name is sumit")
```

Here:

- Key can be line offset or line number conceptually.
- Value is the line content.

Mapper receives this key-value pair.

## Mapper

Mapper takes one input key-value pair and emits intermediate key-value pairs.

For word count:

Input:

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

Important:

Mapper output is not final output. It is intermediate output.

## Reducer

Reducer receives a key and collection of values.

Input:

```text
(hello, [1, 1, 1, 1, 1, 1])
```

Reducer output:

```text
(hello, 6)
```

Reducer is where final aggregation happens.

## How Many Mappers Are Launched?

Simple course rule:

```text
Number of mappers = number of input blocks
```

Example:

```text
File size = 1 GB = 1024 MB
Block size = 128 MB

Number of blocks = 8
Number of mappers = 8
```

If there is a 3-node cluster, still 8 mappers are created. But only some may run in parallel depending on cluster resources.

Interview nuance:

In real Hadoop, mapper count depends on input splits, not always exactly block count. But for simple text-file understanding, block count is a good mental model.

## How Many Reducers Are Launched?

Default reducer count is usually 1.

But as a developer, I can configure reducer count:

- 0 reducers.
- 1 reducer.
- More than 1 reducer.

Reducer count affects:

- Output file count.
- Parallelism.
- Shuffle distribution.
- Runtime.

Output file naming:

```text
With reducer:
    part-r-00000

With zero reducer:
    part-m-00000
```

`r` means reducer output.

`m` means mapper output.

## When To Increase Reducers

Increase reducers when the reduce phase is a bottleneck.

Example:

```text
5 mappers finish in 3 minutes
1 reducer takes 10 minutes

Total time = around 13 minutes
```

If we use 2 reducers:

```text
5 mappers finish in 3 minutes
2 reducers split reduce work

Total time may become around 5.5 to 8 minutes
```

When to increase reducers:

- Reducer is taking much longer than mappers.
- There is a lot of aggregation.
- Data can be distributed well across reducers.
- Multiple output files are acceptable.

When not to increase reducers:

- Data is small.
- Output must be one file.
- Keys are skewed and one reducer still gets most data.
- Too many tiny output files will create downstream problems.

## When To Set Reducers To Zero

Set reducers to 0 when aggregation is not needed.

Example jobs:

- Filter records.
- Convert text format.
- Mask sensitive columns.
- Remove bad records.

With zero reducers:

```text
Mapper output = final output
No shuffle
No sort
No reduce
```

Example:

```text
1 GB file
8 blocks
8 mappers
0 reducers

Final output = 8 mapper output files
```

This is faster for map-only work because shuffle and sort are skipped.

## Partitioner

Partitioner decides which reducer gets a mapper output key.

Partitioning is needed only when reducer count is more than 1.

```text
Mapper output key-value pairs
        |
        v
Partitioner
        |
        +-- Reducer 0
        +-- Reducer 1
        +-- Reducer 2
```

Important:

```text
Number of partitions = number of reducers
```

### Default Hash Partitioning

Default partitioning uses hash logic.

Simple example with 3 reducers:

```text
1 % 3 = 1 -> Reducer 1
2 % 3 = 2 -> Reducer 2
3 % 3 = 0 -> Reducer 0
4 % 3 = 1 -> Reducer 1
```

For string keys, Hadoop uses hash of the key internally.

The key point:

The partitioning function must be consistent.

Consistent means the same key always goes to the same reducer.

Example:

```text
(hello, 1) -> Reducer 1
(hello, 1) -> Reducer 1
(hello, 1) -> Reducer 1
```

If same keys go to different reducers, final aggregation will be wrong.

### Custom Partitioner

Sometimes default hash partitioning is not enough.

Example requirement:

```text
Words with length <= 3 -> Reducer 0
Words with length > 3  -> Reducer 1
```

Input:

```text
hi
hello
how
is
sumit
```

Output distribution:

```text
Reducer 0:
hi, how, is

Reducer 1:
hello, sumit
```

Use custom partitioner when:

- Business logic needs custom grouping.
- Default hash causes skew.
- We need range-based distribution.
- We want specific categories to go to specific reducers.

## Shuffle

Shuffle means moving mapper output from mapper machines to reducer machines.

Why shuffle is needed:

The reducer needs all values for the same key.

Example mapper output:

```text
Mapper 1: (hello, 1), (sumit, 1)
Mapper 2: (hello, 1), (data, 1)
Mapper 3: (sumit, 1), (hello, 1)
```

All `hello` records must go to the same reducer.

Shuffle is expensive because:

- Data moves over network.
- Intermediate data may be written/read from disk.
- Sorting and grouping happen.
- A large shuffle can slow the whole job.

Interview tip:

Whenever a big data job is slow, always think about shuffle.

## Sort

Sort happens on reducer side.

Sort brings same keys together as a collection.

Before sort:

```text
(hi, 1)
(hello, 1)
(hi, 1)
(hello, 1)
(hi, 1)
```

After sort/group:

```text
(hello, [1, 1])
(hi, [1, 1, 1])
```

Reducer sees values as a collection.

## Combiner

Combiner is a mini-reducer that runs on mapper side.

Flow:

```text
Mapper -> Combiner -> Partitioner -> Shuffle -> Sort -> Reducer
```

Why combiner is useful:

- Improves mapper-side work.
- Reduces amount of data sent over network.
- Reduces reducer pressure.

Example without combiner:

```text
Mapper sends:
(hello, 1)
(hello, 1)
(hello, 1)
(hello, 1)
```

Example with combiner:

```text
Mapper sends:
(hello, 4)
```

Less data is shuffled.

### When Combiner Is Safe

Combiner is safe for operations like:

- Sum.
- Count.
- Min.
- Max.

Because partial aggregation gives the same final answer.

### When Combiner Is Dangerous

Combiner is dangerous for average if used incorrectly.

Example:

```text
M1 values = 1, 3, 5
M1 average = 3.0

M2 values = 3, 7, 9
M2 average = 6.33

M3 values = 4, 7, 8, 11
M3 average = 7.5
```

Wrong final average:

```text
(3.0 + 6.33 + 7.5) / 3 = 5.61
```

Correct average:

```text
Total sum = 58
Total count = 10
Average = 5.8
```

Correct combiner design for average:

```text
Mapper/Combiner output = (sum, count)

M1 -> (9, 3)
M2 -> (19, 3)
M3 -> (30, 4)

Reducer -> (58, 10) -> 5.8
```

Important:

Combiner is an optimization, not a place to change final business logic.

## Example 1: LinkedIn Profile Views

Problem:

Find how many views each profile received.

Input:

```text
ID,From-Member,To-Member
1,Manasa,Sumit
2,Deepa,Sumit
3,Sumit,Manasa
4,Manasa,Deepa
5,Deepa,Manasa
6,Shilpy,Manasa
```

Meaning:

```text
1,Manasa,Sumit
```

Manasa viewed Sumit's profile.

Mapper logic:

Take `To-Member` as key and emit `1`.

Mapper output:

```text
(Sumit, 1)
(Sumit, 1)
(Manasa, 1)
(Deepa, 1)
(Manasa, 1)
(Manasa, 1)
```

After shuffle and sort:

```text
(Deepa, [1])
(Manasa, [1, 1, 1])
(Sumit, [1, 1])
```

Reducer output:

```text
(Deepa, 1)
(Manasa, 3)
(Sumit, 2)
```

## Example 2: Max Temperature Per Day

Input:

```text
Date,Time,Temperature
12/12/2015,00:00,50
12/12/2015,01:00,52
12/12/2015,02:00,49
12/13/2015,00:00,48
12/13/2015,01:00,54
12/13/2015,02:00,55
```

Goal:

```text
12/12/2015,52
12/13/2015,55
```

Mapper output:

```text
(12/12/2015, 50)
(12/12/2015, 52)
(12/12/2015, 49)
(12/13/2015, 48)
(12/13/2015, 54)
(12/13/2015, 55)
```

After shuffle and sort:

```text
(12/12/2015, [50, 52, 49])
(12/13/2015, [48, 54, 55])
```

Reducer output:

```text
(12/12/2015, 52)
(12/13/2015, 55)
```

Better optimization:

Use combiner to calculate local max per mapper, then reducer calculates global max.

## Example 3: Google Search Inverted Index

Google used MapReduce-style processing for web search indexing.

Crawler output:

```text
flipkart.com -> clothes handbag laptop
amazon.com   -> clothes mobile purse
myntra.com   -> purse clothes tv
```

Goal:

Build index:

```text
clothes -> flipkart.com, amazon.com, myntra.com
purse   -> amazon.com, myntra.com
```

Mapper output:

```text
(clothes, flipkart.com)
(handbag, flipkart.com)
(laptop, flipkart.com)
(clothes, amazon.com)
(mobile, amazon.com)
(purse, amazon.com)
(purse, myntra.com)
(clothes, myntra.com)
(tv, myntra.com)
```

Reducer output:

```text
(clothes, [flipkart.com, amazon.com, myntra.com])
(purse, [amazon.com, myntra.com])
```

This is called an inverted index.

Instead of:

```text
website -> words
```

we create:

```text
word -> websites
```

That makes search fast.

## Running MapReduce JARs

JAR means Java Archive.

Course JAR location:

```bash
ls /data/ujjawalsingh/mapreduce_jars
```

General command:

```bash
hadoop jar <jar_path> <input_hdfs_path> <output_hdfs_directory>
```

Example:

```bash
hadoop jar /data/ujjawalsingh/mapreduce_jars/mapreduce_prog.jar \
  /user/<username>/data/input/inputfile.txt \
  /user/<username>/data/output
```

Important:

The output directory must not already exist.

Check output:

```bash
hadoop fs -ls /user/<username>/data/output
hadoop fs -cat /user/<username>/data/output/part-r-00000
```

Expected output files:

```text
_SUCCESS
part-r-00000
```

## Course JAR Programs

```text
mapreduce_prog.jar
    Normal word count with reducer

mapreduce_prog_0_reducer.jar
    Zero reducer, mapper output is final output

mapreduce_prog_2_reducer.jar
    Two reducers, default hash partitioning

mapreduce_prog_cpartitioner.jar
    Custom partitioner

mapreduce_prog_combiner.jar
    Word count with combiner
```

## Common Mistakes

- Thinking mapper output is final output.
- Forgetting shuffle and sort happen before reducer.
- Thinking reducer is always required.
- Increasing reducers without checking key distribution.
- Using combiner for average incorrectly.
- Creating output directory before running a MapReduce job.
- Forgetting that same key must go to same reducer.
- Confusing partitioning with shuffle.

## Best Practices

- Do maximum safe computation on mapper side.
- Use combiner when operation is safe.
- Avoid unnecessary shuffle.
- Increase reducers when reduce phase is bottleneck.
- Use zero reducers for map-only jobs.
- Use custom partitioner if default hash creates skew.
- Monitor map output records, reduce input records, and shuffle bytes.

## Performance Tips

- Shuffle is usually the most expensive part.
- Combiner can reduce network transfer.
- Bad partitioning can make one reducer overloaded.
- Too many reducers can create too many small files.
- Too few reducers can create bottlenecks.
- Output directory cleanup is needed before reruns.

## Interview Questions

### Beginner Questions

- What is MapReduce?
- What is mapper?
- What is reducer?
- What is shuffle?
- What is sort?
- What is combiner?

### Intermediate Questions

- How many mappers run for a 1 GB file with 128 MB block size?
- When should reducers be increased?
- When should reducers be set to zero?
- What is partitioner?
- What is custom partitioning?
- Why is shuffle expensive?

### Senior Data Engineer Questions

- How do you reduce shuffle cost?
- How do you decide reducer count?
- How do you handle reducer skew?
- Why is combiner unsafe for average?
- How would you design an inverted index using MapReduce?

## Scenario-Based Questions

### Scenario 1: Reducer Bottleneck

Your mappers finish in 2 minutes, but reducer takes 20 minutes. What will you do?

Answer:

- Check reducer input size.
- Check key skew.
- Increase reducer count if data can be split.
- Add combiner if aggregation is safe.
- Reduce mapper output volume.
- Use custom partitioner if one key range is overloaded.

### Scenario 2: Filter Job

You only need to filter bad rows from a file. Do you need reducer?

Answer:

No. This is a map-only job. Set reducers to zero to avoid shuffle and sort.

### Scenario 3: Average Calculation

Can combiner be used for average?

Answer:

Not by averaging averages. Use `(sum, count)` as intermediate output, then calculate final average in reducer.

## Quick Revision

```text
Map = parallelism
Reduce = aggregation

Flow:
Record Reader -> Mapper -> Combiner -> Partitioner -> Shuffle -> Sort -> Reducer

0 reducers = no shuffle/sort
More reducers = more reduce parallelism
Combiner = local aggregation
Partitioner = decides reducer
Shuffle = mapper output moves to reducer
Sort = same keys grouped together
```
