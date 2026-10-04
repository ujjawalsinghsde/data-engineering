# RDD Transformations And Actions

## Introduction

This section goes deeper into Spark Core API, mainly RDD operations.

Earlier, I learned:

```text
textFile -> flatMap -> map -> reduceByKey -> action
```

The goal now is to understand more RDD operations and how they behave internally:

- Lambda functions.
- Higher order functions.
- Python `map` vs Spark `map`.
- `parallelize`.
- `map`, `filter`, `flatMap`.
- `reduce`.
- `reduceByKey`.
- `countByValue`.
- `distinct`.
- `sortBy`.
- Narrow vs wide transformations.
- Jobs, stages, and tasks.

This week is important because it teaches how Spark actually behaves, not just how to write code.

## Normal Function Vs Lambda Function

### Normal Function

A normal function has a name and can be reused.

```python
def my_sum(x, y):
    return x + y

total = my_sum(5, 7)
print(total)
```

Expected output:

```text
12
```

Use normal function when:

- Logic is reused.
- Logic is long.
- Function needs clear name.
- Debugging matters.

### Lambda Function

Lambda is an anonymous function.

Anonymous means it has no formal function name.

```python
lambda x, y: x + y
```

Example:

```python
from functools import reduce

my_list = [5, 7, 8, 2, 5, 9]
total = reduce(lambda x, y: x + y, my_list)
print(total)
```

Expected output:

```text
36
```

Use lambda when:

- Logic is small.
- Logic is used only once.
- It is passed directly into a higher order function.

Common Spark example:

```python
rdd.map(lambda x: x.lower())
```

## Higher Order Functions

A higher order function is a function that:

- Takes another function as input, or
- Returns another function as output.

Examples:

- `map`
- `filter`
- `reduce`
- Spark transformations like `rdd.map(...)`

Example:

```python
numbers = [1, 2, 3]
result = map(lambda x: x * 2, numbers)
print(list(result))
```

Expected output:

```text
[2, 4, 6]
```

Here `map` is a higher order function because it takes `lambda x: x * 2` as input.

## Python Map Vs Spark Map

This is a key concept.

### Python `map`

Python `map` runs on one machine.

```python
numbers = [1, 2, 3, 4]
result = list(map(lambda x: x * 2, numbers))
```

Output:

```text
[2, 4, 6, 8]
```

This is local processing.

### Spark `map`

Spark `map` is a distributed transformation.

```python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
result = rdd.map(lambda x: x * 2)
result.collect()
```

Output:

```text
[2, 4, 6, 8]
```

Difference:

| Point | Python `map` | Spark `map` |
|---|---|---|
| Runs on | Single machine | Cluster |
| Input | Python collection | RDD partitions |
| Execution | Immediate when iterated | Lazy until action |
| Scale | Small/local data | Large/distributed data |

## SparkSession Boilerplate

Use this for notebooks:

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

## `parallelize`

`parallelize` creates an RDD from a local Python collection.

It is very useful during development.

Real project thought process:

```text
1. Create small sample data locally.
2. Build and test logic using parallelize.
3. Replace sample RDD with real file from HDFS/S3/ADLS.
4. Run same logic on large data.
```

Example:

```python
words = ("big", "Data", "Is", "SUPER", "Interesting", "BIG", "data", "IS", "A", "Trending", "technology")

words_rdd = spark.sparkContext.parallelize(words)
```

Normalize and count:

```python
result = (
    spark.sparkContext
    .parallelize(words)
    .map(lambda x: x.lower())
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
)

result.collect()
```

Expected output:

```text
[('big', 2), ('data', 2), ('is', 2), ('super', 1), ('interesting', 1), ('a', 1), ('trending', 1), ('technology', 1)]
```

Order may differ because RDD output ordering is not guaranteed unless sorted.

## `map`

`map` transforms each input record into one output record.

```text
1000 input rows -> 1000 output rows
```

Example:

```python
rdd = spark.sparkContext.parallelize(["BIG", "Data"])
rdd2 = rdd.map(lambda x: x.lower())
rdd2.collect()
```

Output:

```text
['big', 'data']
```

For orders:

```python
orders_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/orders/*")
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3], 1))
```

Input:

```text
1,2013-07-25 00:00:00.0,11599,CLOSED
```

Output:

```text
('CLOSED', 1)
```

## `filter`

`filter` keeps records that satisfy a condition.

```text
1000 input rows -> 0 to 1000 output rows
```

Example:

```python
closed_orders = orders_rdd.filter(lambda x: x.split(",")[3] == "CLOSED")
```

Use when:

- Need records matching condition.
- Want to reduce data before expensive transformations.

Best practice:

Filter early to reduce data before wide transformations like `reduceByKey` or `join`.

## `flatMap`

`flatMap` maps one input record into many output records and flattens the result.

Example:

```python
lines = spark.sparkContext.parallelize(["big data", "spark core"])
words = lines.flatMap(lambda x: x.split(" "))
words.collect()
```

Output:

```text
['big', 'data', 'spark', 'core']
```

Difference between `map` and `flatMap`:

```python
lines.map(lambda x: x.split(" ")).collect()
```

Output:

```text
[['big', 'data'], ['spark', 'core']]
```

`flatMap` is used for word count because I need one flat stream of words.

## `reduce`

`reduce` is an action.

It aggregates all RDD elements into one final value.

Example:

```python
my_list = [1, 4, 6, 8, 9, 10, 12]
base_rdd = spark.sparkContext.parallelize(my_list)

total = base_rdd.reduce(lambda x, y: x + y)
print(total)
```

Expected output:

```text
50
```

Important:

`reduce` returns one value to the driver.

Use only when final result is small.

## `reduceByKey`

`reduceByKey` is a transformation.

It works on pair RDDs.

Pair RDD format:

```text
(key, value)
```

Example:

```python
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
```

Input:

```text
('CLOSED', 1)
('PENDING_PAYMENT', 1)
('CLOSED', 1)
```

Output:

```text
('CLOSED', 2)
('PENDING_PAYMENT', 1)
```

If there are 14 different order statuses, `reduceByKey` gives 14 output records.

## `reduce` Vs `reduceByKey`

| Point | `reduce` | `reduceByKey` |
|---|---|---|
| Type | Action | Transformation |
| Works on | Any RDD | Pair RDD |
| Output | Single value | RDD with one result per key |
| Example | Sum all numbers | Count orders by status |

Example:

```python
numbers_rdd.reduce(lambda x, y: x + y)
```

returns:

```text
one number
```

Example:

```python
pair_rdd.reduceByKey(lambda x, y: x + y)
```

returns:

```text
RDD of (key, aggregated_value)
```

## `countByValue`

`countByValue` is an action.

It counts frequency of each distinct value.

Example:

```python
status_rdd = orders_rdd.map(lambda x: x.split(",")[3])
status_rdd.countByValue()
```

This can replace:

```python
orders_rdd.map(lambda x: (x.split(",")[3], 1)).reduceByKey(lambda x, y: x + y)
```

Important difference:

`countByValue` brings result to driver as local object.

Use `countByValue` when:

- Final result is small.
- No further distributed processing is needed.

Use `map + reduceByKey` when:

- Need further transformations.
- Result may be large.
- Want to keep processing distributed.

## `distinct`

`distinct` returns unique values.

Example:

```python
distinct_customers = orders_rdd.map(lambda x: x.split(",")[2]).distinct()
distinct_customers.count()
```

Use case:

Count customers who placed at least one order.

Note:

`distinct` is a wide transformation because Spark needs to compare values across partitions.

## `sortBy`

`sortBy` sorts an RDD.

Example:

```python
customers_sorted = customers_aggregated.sortBy(lambda x: x[1], ascending=False)
customers_sorted.take(10)
```

Use for:

- Top N records.
- Ranking.

Performance note:

Global sort is expensive because it involves shuffle.

For top N, `takeOrdered` or `top` can sometimes be more efficient, but this course uses `sortBy(...).take(...)`, which is easier to understand.

## Orders Use Cases

Orders schema:

```text
order_id, order_date, customer_id, order_status
```

Load:

```python
orders_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/orders/*")
```

### 1. Count Orders Under Each Status

```python
mapped_rdd = orders_rdd.map(lambda x: (x.split(",")[3], 1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
reduced_sorted = reduced_rdd.sortBy(lambda x: x[1], ascending=False)
reduced_sorted.collect()
```

### 2. Top 10 Premium Customers By Order Count

```python
customers_mapped = orders_rdd.map(lambda x: (x.split(",")[2], 1))
customers_aggregated = customers_mapped.reduceByKey(lambda x, y: x + y)
customers_sorted = customers_aggregated.sortBy(lambda x: x[1], ascending=False)
customers_sorted.take(10)
```

Here premium means customers who placed the most orders.

### 3. Distinct Count Of Customers

```python
distinct_customers = orders_rdd.map(lambda x: x.split(",")[2]).distinct()
distinct_customers.count()
```

### 4. Customers With Maximum CLOSED Orders

```python
filtered_orders = orders_rdd.filter(lambda x: x.split(",")[3] == "CLOSED")
filtered_mapped = filtered_orders.map(lambda x: (x.split(",")[2], 1))
filtered_aggregated = filtered_mapped.reduceByKey(lambda x, y: x + y)
filtered_sorted = filtered_aggregated.sortBy(lambda x: x[1], ascending=False)
filtered_sorted.take(10)
```

## Narrow Vs Wide Transformations

All transformations fall into two categories.

### Narrow Transformation

No shuffle.

Each output partition depends on one input partition.

Examples:

- `map`
- `filter`
- `flatMap`

Diagram:

```text
Partition 1 -> map/filter -> Partition 1
Partition 2 -> map/filter -> Partition 2
Partition 3 -> map/filter -> Partition 3
```

### Wide Transformation

Shuffle happens.

Data moves between machines.

Examples:

- `reduceByKey`
- `groupByKey`
- `join`
- `distinct`
- `sortBy`

Diagram:

```text
Partition 1 \
Partition 2  -> shuffle -> grouped output partitions
Partition 3 /
```

Best practice:

Minimize wide transformations and push them as late as possible.

Example:

Better:

```text
Load -> filter -> map -> reduceByKey
```

Worse:

```text
Load -> reduceByKey -> filter
```

## Jobs, Stages, And Tasks

### Job

One action triggers one Spark job.

Examples:

```python
rdd.collect()
rdd.count()
rdd.saveAsTextFile(...)
```

Each action creates a job.

### Stage

Stages are separated by wide transformations.

Rule of thumb:

```text
Number of stages = number of wide transformations + 1
```

Example:

```text
load -> map -> reduceByKey -> collect
```

There is one wide transformation: `reduceByKey`.

So:

```text
Stages = 2
```

### Task

Task is work done on one partition.

Rule:

```text
Number of tasks = number of partitions in that stage
```

Example:

If RDD has 8 partitions, a stage may have 8 tasks.

## Important Points To Remember

- `map` keeps same number of records.
- `filter` reduces or keeps records.
- `flatMap` can produce many records per input.
- `reduce` returns one value and is an action.
- `reduceByKey` returns one record per key and is a transformation.
- `countByValue` is an action and returns result to driver.
- Wide transformations cause shuffle.
- Shuffle is expensive.
- One action creates one job.
- Wide transformations split stages.
- Number of tasks depends on partitions.

## Common Mistakes

- Using `collect()` on large RDD.
- Using `groupByKey` when `reduceByKey` is enough.
- Calling many actions without caching reused RDDs.
- Doing wide transformations before filtering.
- Expecting deterministic order without sorting.
- Confusing Python `map` with Spark `map`.
- Using `countByValue` when result is huge.

## Best Practices

- Develop on sample data using `parallelize`.
- Replace sample data with large file path after logic works.
- Filter early.
- Use `reduceByKey` for aggregation.
- Use `take()` for preview.
- Use `saveAsTextFile()` for large outputs.
- Use Spark History Server to inspect jobs and stages.

## Interview Questions

### Beginner Questions

- What is a lambda function?
- What is a higher order function?
- What is `parallelize`?
- What is the difference between `map` and `flatMap`?
- What is the difference between `reduce` and `reduceByKey`?

### Intermediate Questions

- What is `countByValue`?
- When should we avoid `countByValue`?
- What are narrow transformations?
- What are wide transformations?
- What is a Spark job?
- What is a Spark stage?
- What is a Spark task?

### Senior Data Engineer Questions

- Why should wide transformations be delayed?
- How do partitions affect task count?
- How do you debug stage explosion in Spark UI?
- Why can `sortBy` be expensive?
- How would you design RDD logic for a 10 TB file?

## Quick Revision

```text
map: 1 row -> 1 row
filter: 1 row -> 0/1 row
flatMap: 1 row -> many rows
reduce: full RDD -> one value
reduceByKey: pair RDD -> one value per key
countByValue: value counts returned to driver

Narrow = no shuffle
Wide = shuffle
Job = action
Stage = split by wide transformation
Task = one partition work
```
