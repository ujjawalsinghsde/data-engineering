# Window Functions, Rank, Lead, And Lag

## Introduction

Normal `groupBy` collapses rows.

Window functions calculate values across related rows while keeping individual rows.

Example:

If I use `groupBy("country").sum("invoicevalue")`, I get one row per country.

If I use a window function, I can keep every row and add a new column like:

```text
running_total
rank
previous_week_value
next_week_value
```

This is very important for analytics and interview questions.

## GroupBy Vs Window

Input:

```text
country   weeknum   invoicevalue
Germany   48        3309.75
Germany   49        4521.39
Germany   50        5065.79
```

GroupBy:

```text
Germany   total_invoice_value
```

Window:

```text
Germany   48   3309.75   running_total=3309.75
Germany   49   4521.39   running_total=7831.14
Germany   50   5065.79   running_total=12896.93
```

Window keeps row-level detail.

## Window Specification

A window usually has three parts:

1. Partition column
2. Ordering column
3. Window frame

Example:

```python
from pyspark.sql.window import Window

mywindow = (
    Window
    .partitionBy("country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
```

Meaning:

- Partition by `country`: calculate separately for each country.
- Order by `weeknum`: arrange rows by week.
- Rows between beginning and current row: running calculation from first row to current row.

## Load Window Dataset

Dataset:

```text
/public/trendytech/datasets/windowdata.csv
```

Read:

```python
orders_df = (
    spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("/public/trendytech/datasets/windowdata.csv")
)

orders_df.sort("country").show()
```

Production note:

Use explicit schema in real jobs.

## Running Total

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

mywindow = (
    Window
    .partitionBy("country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result_df = orders_df.withColumn(
    "running_total",
    sum("invoicevalue").over(mywindow)
)

result_df.show()
```

## How Running Total Works

For each country:

```text
week 48 -> sum week 48
week 49 -> sum week 48 to 49
week 50 -> sum week 48 to 50
week 51 -> sum week 48 to 51
```

This is useful for:

- cumulative sales
- running revenue
- running customer spend
- cumulative log counts
- stock balance calculation

## Window Without Frame

Sometimes frame is not needed.

Example:

```python
country_window = Window.partitionBy("country")

result = orders_df.withColumn(
    "total_invoice_value",
    sum("invoicevalue").over(country_window)
)
```

This gives total invoice value for each country on every row.

It does not collapse rows like `groupBy`.

## Ranking Functions

Common ranking functions:

- `rank`
- `dense_rank`
- `row_number`

They are used with window ordering.

Example dataset:

```text
/public/trendytech/datasets/windowdatamodified.csv
```

Read:

```python
orders_df = (
    spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("/public/trendytech/datasets/windowdatamodified.csv")
)
```

## Rank

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

rank_window = (
    Window
    .partitionBy("country")
    .orderBy(desc("invoicevalue"))
)

ranked_df = orders_df.withColumn(
    "rank",
    rank().over(rank_window)
)

ranked_df.show()
```

`rank()` gives same rank to ties but skips next ranks.

Example:

```text
name     score   rank
Ankur    100     1
Satish   100     1
Kapil    100     1
Kaushik   99     4
Ram       99     4
Rohit     98     6
```

Ranks 2 and 3 are skipped because three people share rank 1.

## Dense Rank

```python
from pyspark.sql.functions import dense_rank

dense_ranked_df = orders_df.withColumn(
    "dense_rank",
    dense_rank().over(rank_window)
)
```

`dense_rank()` gives same rank to ties but does not skip ranks.

Example:

```text
name     score   dense_rank
Ankur    100     1
Satish   100     1
Kapil    100     1
Kaushik   99     2
Ram       99     2
Rohit     98     3
```

## Row Number

```python
from pyspark.sql.functions import row_number

rownum_df = orders_df.withColumn(
    "row_number",
    row_number().over(rank_window)
)
```

`row_number()` gives a unique number to every row.

Even if there is a tie, row numbers are different.

Example:

```text
name     score   row_number
Ankur    100     1
Satish   100     2
Kapil    100     3
Kaushik   99     4
```

Important:

If ordering column has ties and no secondary sort column is used, `row_number()` can be non-deterministic.

Better:

```python
rank_window = (
    Window
    .partitionBy("country")
    .orderBy(desc("invoicevalue"), "weeknum")
)
```

## Rank Vs Dense Rank Vs Row Number

| Function | Tie Handling | Skips Rank? | Unique Per Row? |
|---|---|---|---|
| `rank` | same rank for ties | yes | no |
| `dense_rank` | same rank for ties | no | no |
| `row_number` | different number | no | yes |

## Top N Per Group

Top invoice per country:

```python
top_df = (
    orders_df
    .withColumn("rn", row_number().over(rank_window))
    .filter("rn = 1")
    .drop("rn")
)

top_df.show()
```

Use `row_number` when I need exactly one row per group.

Use `rank` when ties should all be included.

Example:

```python
top_with_ties = (
    orders_df
    .withColumn("rank", rank().over(rank_window))
    .filter("rank = 1")
)
```

## Lead And Lag

`lead` and `lag` compare current row with next or previous row.

Use cases:

- week-over-week sales difference
- previous transaction amount
- next event timestamp
- customer behavior change
- stock price comparison

## Lag

Lag gives previous row value.

```python
from pyspark.sql.functions import lag, expr

week_window = (
    Window
    .partitionBy("country")
    .orderBy("weeknum")
)

results_df = orders_df.withColumn(
    "previous_week",
    lag("invoicevalue").over(week_window)
)

final_df = results_df.withColumn(
    "invoice_diff",
    expr("invoicevalue - previous_week")
)

final_df.show()
```

For the first row in each partition, `previous_week` is null.

## Lead

Lead gives next row value.

```python
from pyspark.sql.functions import lead

result_df = orders_df.withColumn(
    "next_week",
    lead("invoicevalue").over(week_window)
)

result_df.show()
```

For the last row in each partition, `next_week` is null.

## Lead/Lag With Default Value

```python
orders_df.withColumn(
    "previous_week",
    lag("invoicevalue", 1, 0).over(week_window)
).show()
```

Arguments:

```text
lag(column, offset, default_value)
```

## Window Frame Types

Two common frame types:

```python
rowsBetween()
rangeBetween()
```

## `rowsBetween`

Uses physical row positions.

Example:

```python
rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

Means:

```text
from first row in partition to current row
```

## `rangeBetween`

Uses value range based on ordering column.

Useful for numeric/date ranges.

Can be tricky when there are duplicate order values.

For most beginner/intermediate use cases, `rowsBetween` is easier to reason about.

## Window Function Performance

Window functions can be expensive because Spark must:

- partition data by window key
- sort data inside each partition
- maintain frame calculations

Performance tips:

- filter data before window
- select only required columns
- partition by meaningful keys
- avoid massive skewed partitions
- use deterministic order by
- avoid unnecessary wide windows

## Common Mistakes

- Using window when `groupBy` is enough.
- Forgetting `partitionBy`, causing global window and huge single partition behavior.
- Using `row_number` without deterministic tie-breaker.
- Confusing `rank` and `dense_rank`.
- Forgetting first lag value is null.
- Sorting in wrong direction.
- Not realizing window requires shuffle/sort.

## Best Practices

- Always define partition key clearly.
- Always define order key clearly.
- Add secondary order columns for deterministic results.
- Use `row_number` for one top row per group.
- Use `rank` when ties should be preserved.
- Use `lag` for previous-row comparison.
- Use `lead` for next-row comparison.
- Filter and select before window calculations.

## Interview Questions

### Beginner Questions

- What is a window function?
- How is window different from groupBy?
- What is running total?
- What is `partitionBy` in window?
- What is `orderBy` in window?

### Intermediate Questions

- Explain `rank`, `dense_rank`, and `row_number`.
- How do you find top 1 record per group?
- What is the difference between `lead` and `lag`?
- Why does first row have null for lag?
- Why can window functions be expensive?

### Senior Data Engineer Questions

- How would you optimize window functions on a large dataset?
- How would you handle skew in window partitions?
- How do you make `row_number` deterministic?
- When would you use window instead of self-join?
- How would you calculate rolling 7-day revenue?

## Scenario-Based Questions

### Scenario 1: Need Top Customer Per Country

Use:

```python
Window.partitionBy("country").orderBy(desc("revenue"))
row_number()
```

If ties should be included, use `rank()`.

### Scenario 2: Week-Over-Week Sales Difference

Use:

```python
lag("sales").over(Window.partitionBy("country").orderBy("weeknum"))
```

Then subtract previous value from current value.

### Scenario 3: Window Job Very Slow

Check:

- partition key skew
- ordering sort cost
- input size
- number of shuffle partitions
- unnecessary columns
- missing filters

## Real Project Perspective

Window functions are heavily used in production for:

- deduplication
- latest record selection
- customer ranking
- sessionization
- running balances
- time-based comparisons
- fraud detection patterns

Example dedup pattern:

```python
from pyspark.sql.functions import row_number, desc

dedup_window = Window.partitionBy("order_id").orderBy(desc("updated_at"))

deduped_df = (
    orders_df
    .withColumn("rn", row_number().over(dedup_window))
    .filter("rn = 1")
    .drop("rn")
)
```

This keeps the latest record per order.

## Quick Revision

- Window keeps rows; groupBy collapses rows.
- Window needs partition, order, and sometimes frame.
- Running total uses `sum().over(window)`.
- `rank` skips ranks after ties.
- `dense_rank` does not skip ranks.
- `row_number` gives unique row numbers.
- `lag` compares with previous row.
- `lead` compares with next row.
