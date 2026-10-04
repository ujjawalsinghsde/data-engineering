# Log Analysis, Pivot Tables, And Null Handling

## Introduction

This topic combines three very practical skills:

- analyzing log files
- creating pivot views
- handling nulls in Spark

These are common in real data engineering work.

Logs are semi-structured operational data.

Pivot tables make aggregate results easier to read.

Null handling is required in almost every production dataset.

## Log Analysis Use Case

Input log data:

```text
INFO,2015-8-8 20:49:22
WARN,2015-1-14 20:05:00
INFO,2017-6-14 00:08:35
DEBUG,2017-7-1 12:55:02
```

Goal:

Find log count by:

- log level
- month

Example output:

```text
loglevel   month      total_occurrences
INFO       January    10000
ERROR      January    2000
WARN       February   3000
```

## Develop Logic On Sample Data First

Before running on 1 million records, first test on small sample data.

```python
logs_data = [
    ("DEBUG", "2014-6-22 21:30:49"),
    ("WARN", "2013-12-6 17:54:15"),
    ("DEBUG", "2017-1-12 10:47:02"),
    ("DEBUG", "2016-6-25 11:06:42"),
    ("ERROR", "2015-6-28 19:25:05"),
    ("DEBUG", "2012-6-24 01:06:37"),
    ("INFO", "2014-12-9 09:53:54"),
    ("DEBUG", "2015-11-8 19:20:08"),
    ("INFO", "2017-12-21 18:34:18"),
]

log_df = spark.createDataFrame(logs_data).toDF("loglevel", "logtime")
log_df.show()
log_df.printSchema()
```

Initially `logtime` is string.

Convert to timestamp:

```python
from pyspark.sql.functions import to_timestamp

new_log_df = log_df.withColumn(
    "logtime",
    to_timestamp("logtime")
)

new_log_df.printSchema()
new_log_df.show()
```

## Create Temp View

```python
new_log_df.createOrReplaceTempView("serverlogs")
```

Extract month:

```python
spark.sql("""
    SELECT
        loglevel,
        date_format(logtime, 'MMMM') AS month
    FROM serverlogs
""").show()
```

Aggregate:

```python
spark.sql("""
    SELECT
        loglevel,
        date_format(logtime, 'MMMM') AS month,
        COUNT(*) AS total_occurrences
    FROM serverlogs
    GROUP BY loglevel, month
""").show()
```

## Apply Logic To Full Dataset

Dataset:

```text
/public/trendytech/datasets/logdata1m.csv
```

Schema:

```python
log_schema = """
loglevel string,
logtime timestamp
"""

log_df = (
    spark.read
    .format("csv")
    .schema(log_schema)
    .load("/public/trendytech/datasets/logdata1m.csv")
)

log_df.createOrReplaceTempView("serverlogs")
```

Run aggregation:

```python
spark.sql("""
    SELECT
        loglevel,
        date_format(logtime, 'MMMM') AS month,
        COUNT(*) AS total_occurrences
    FROM serverlogs
    GROUP BY loglevel, month
""").show()
```

## Sorting Months Correctly

If I order by month name:

```sql
ORDER BY month
```

Spark sorts alphabetically:

```text
April
August
December
February
```

That is not calendar order.

Better:

```python
result_df = spark.sql("""
    SELECT
        loglevel,
        date_format(logtime, 'MMMM') AS month,
        first(date_format(logtime, 'MM')) AS month_num,
        COUNT(*) AS total_occurrences
    FROM serverlogs
    GROUP BY loglevel, month
    ORDER BY month_num
""")

final_df = result_df.drop("month_num")
final_df.show(60)
```

Why `MM`?

It gives month number with leading zero:

```text
01, 02, 03, ..., 12
```

This sorts correctly as string.

## Pivot Table

Pivot converts row values into columns.

Before pivot:

```text
loglevel   month      count
INFO       January    100
INFO       February   120
ERROR      January    20
ERROR      February   25
```

After pivot:

```text
loglevel   January   February
INFO       100       120
ERROR      20        25
```

## Pivot Example

```python
month_df = spark.sql("""
    SELECT
        loglevel,
        date_format(logtime, 'MMMM') AS month
    FROM serverlogs
""")

month_df.groupBy("loglevel").pivot("month").count().show()
```

## Pivot Optimization

If I do:

```python
.pivot("month")
```

Spark may scan data to find all distinct month values.

Better:

```python
month_list = [
    "January", "February", "March", "April",
    "May", "June", "July", "August",
    "September", "October", "November", "December"
]

month_df.groupBy("loglevel").pivot("month", month_list).count().show()
```

Why this is faster:

Spark does not need an extra job to discover pivot values.

## Pivot Mistake

If the values in `month_list` do not match actual data, output will show nulls.

Example:

```python
month_list = ["Jan", "February", "March"]
```

But actual data has:

```text
January
```

Then `Jan` column will not match January records.

Always make pivot values exact.

## When To Use Pivot

Use pivot when:

- reporting needs cross-tab view
- number of pivot values is small and known
- dashboard needs columns like months/statuses/categories

Avoid pivot when:

- pivot column has thousands of values
- schema would become too wide
- values are unpredictable
- downstream can handle long format better

## Null Handling In Spark

Null means missing or unknown value.

It is not the same as:

- empty string
- zero
- false
- blank space

Example:

```text
customer_id, email
101, abc@example.com
102, null
103, ""
```

`null` means no value.

`""` means empty string value.

## Create Sample Data With Nulls

```python
data = [
    (1, "Amit", 1000.0, "Mumbai"),
    (2, "Riya", None, "Delhi"),
    (3, None, 500.0, None),
    (4, "Kabir", 700.0, ""),
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary", "city"]
)

df.show()
```

## Detect Nulls

```python
from pyspark.sql.functions import col

df.filter(col("salary").isNull()).show()
df.filter(col("salary").isNotNull()).show()
```

SQL style:

```python
df.filter("salary IS NULL").show()
df.filter("salary IS NOT NULL").show()
```

## Count Nulls Per Column

```python
from pyspark.sql.functions import sum, when, col

null_counts_df = df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
])

null_counts_df.show()
```

This is useful for data quality checks.

## Drop Nulls

Drop rows with any null:

```python
df.na.drop().show()
```

Drop rows where all values are null:

```python
df.na.drop(how="all").show()
```

Drop based on selected columns:

```python
df.na.drop(subset=["name", "salary"]).show()
```

Use carefully.

Dropping rows can lose business data.

## Fill Nulls

Fill all compatible columns:

```python
df.na.fill(0).show()
```

Fill specific columns:

```python
df.na.fill({
    "salary": 0.0,
    "name": "UNKNOWN",
    "city": "UNKNOWN"
}).show()
```

## Replace Empty Strings With Null

Sometimes source data has empty strings.

```python
from pyspark.sql.functions import when, trim

clean_df = df.withColumn(
    "city",
    when(trim(col("city")) == "", None).otherwise(col("city"))
)
```

Then fill:

```python
clean_df.na.fill({"city": "UNKNOWN"}).show()
```

## Nulls In Comparisons

This is important.

```python
df.filter("salary = NULL").show()
```

This is wrong.

Use:

```python
df.filter("salary IS NULL").show()
```

In SQL, null means unknown.

Any normal comparison with null does not behave like comparing normal values.

## Null-Safe Equality

Spark supports null-safe equality:

```python
df.filter(col("city").eqNullSafe("Delhi")).show()
```

In SQL:

```sql
city <=> 'Delhi'
```

Null-safe equality is useful in joins when nulls should be matched explicitly.

## Nulls In Aggregations

Aggregations behave differently:

```python
from pyspark.sql.functions import count, avg, sum

df.select(
    count("*").alias("all_rows"),
    count("salary").alias("non_null_salary"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary")
).show()
```

Remember:

- `count("*")` counts all rows
- `count("salary")` counts only non-null salaries
- `avg("salary")` ignores nulls
- `sum("salary")` ignores nulls

## Nulls In Joins

Normal joins do not match null keys.

Example:

```text
left.customer_id = null
right.customer_id = null
```

These usually do not match in normal equality joins.

If business wants null-safe matching, use null-safe equality carefully.

But in most data models, null join keys should be cleaned or rejected before joining.

## Production Null Handling Strategy

Common approach:

```text
Raw Data
   |
   v
Profile null counts
   |
   v
Apply business rules
   |
   +-- required column null -> reject/quarantine
   |
   +-- optional column null -> fill default or keep null
   |
   v
Clean Data
```

Not all nulls are bad.

Example:

- `middle_name` null is okay
- `customer_id` null is usually not okay
- `transaction_amount` null may be invalid

## Assignment Ideas

The assignment asks to:

- research Uber mode
- observe multi-node cluster ResourceManager
- choose a dataset and demonstrate aggregations/windows/pivot
- explain null handling

Suggested dataset:

Use retail or order data because it already has:

- country
- invoice
- quantity
- price
- date

Suggested queries:

- total revenue by country
- running revenue by country over date/week
- top invoice per country using `row_number`
- rank countries by revenue
- previous week revenue using `lag`
- pivot revenue by month

## Common Mistakes

- Sorting month names alphabetically instead of calendar order.
- Using pivot without specifying known values, causing extra scan.
- Pivoting on high-cardinality columns.
- Treating empty string as null automatically.
- Using `= NULL` instead of `IS NULL`.
- Dropping all rows with nulls without business approval.
- Filling numeric nulls with zero when zero has business meaning.

## Best Practices

- Develop log logic on sample data first.
- Convert timestamp strings explicitly.
- Use `date_format` carefully.
- Use month number for sorting.
- Provide pivot value list when possible.
- Profile null counts before cleaning.
- Handle required and optional columns differently.
- Keep rejected records for audit.

## Interview Questions

### Beginner Questions

- What is a pivot table?
- How do you extract month from timestamp?
- What is null in Spark?
- How do you filter null values?
- How do you fill nulls?

### Intermediate Questions

- Why should pivot values be provided explicitly?
- Why does ordering by month name give wrong order?
- What is the difference between null and empty string?
- How do nulls behave in aggregations?
- How do you count nulls per column?

### Senior Data Engineer Questions

- How would you design log analytics at scale?
- How would you handle nulls in a financial transaction pipeline?
- When would you keep nulls instead of filling them?
- How do nulls affect joins and metrics?
- How would you monitor data quality for null spikes?

## Scenario-Based Questions

### Scenario 1: Pivot Job Is Slow

Possible reasons:

- Spark scans distinct pivot values.
- Pivot column has many values.
- Data was not filtered.
- Shuffle is large.

Fix:

- provide pivot value list
- filter data
- reduce columns
- avoid pivot on high-cardinality columns

### Scenario 2: Monthly Log Output Sorted Wrong

Reason:

Month names are strings.

Fix:

Sort by month number:

```sql
first(date_format(logtime, 'MM')) AS month_num
```

### Scenario 3: Null Counts Suddenly Increase

Possible reasons:

- source schema changed
- parsing failed
- upstream system sent blanks
- bad file arrived
- date/number format changed

Production response:

- alert
- quarantine bad records
- compare with previous batch
- notify source owner

## Real Project Perspective

Log analytics is used for:

- system monitoring
- error trend analysis
- SLA reporting
- security event detection
- application debugging

Pivot is used for:

- reporting
- monthly trend tables
- dashboard-ready summaries

Null handling is used everywhere.

Bad null handling can silently corrupt reports.

Example:

If null `transaction_amount` is filled with zero without approval, revenue reports may look lower than reality.

## Quick Revision

- Develop logic on small sample first.
- Convert log timestamp string to timestamp.
- Use `date_format` to extract month.
- Sort months by month number.
- Pivot turns row values into columns.
- Provide pivot values for better performance.
- Null is not zero or empty string.
- Use `isNull`, `isNotNull`, `na.fill`, `na.drop`.
- `count(column)` ignores nulls.
