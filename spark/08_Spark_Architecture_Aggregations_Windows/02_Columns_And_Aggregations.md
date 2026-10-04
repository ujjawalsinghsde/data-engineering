# Columns And Aggregations In PySpark

## Introduction

DataFrame work depends heavily on columns.

In PySpark, there are multiple ways to reference columns.

This feels confusing at first, but each style has a use case.

This note covers:

- column access styles
- simple aggregations
- grouping aggregations
- programmatic API
- `selectExpr`
- Spark SQL style

## Load Orders Data

Example schema:

```python
orders_schema = """
order_id long,
order_date date,
cust_id long,
order_status string
"""

orders_df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/trendytech/orders/orders_1gb.csv")
)
```

Show data:

```python
orders_df.show()
orders_df.printSchema()
```

## Ways To Access Columns

PySpark column access styles:

1. Column string
2. DataFrame dot notation
3. DataFrame array notation
4. Column object using `col()` or `column()`
5. Column expression using `expr()`

## 1. Column String

```python
orders_df.select("order_id", "order_date").show()
```

This is simple and commonly used.

Use when:

- selecting columns
- column names are simple
- no ambiguity

## 2. DataFrame Dot Notation

```python
orders_df.select(orders_df.order_date).show()
```

Use when:

- I want to clearly refer to a column from a specific DataFrame
- useful in joins where same column exists in both DataFrames

Limitation:

Dot notation may not work well when column names contain spaces, special characters, or conflict with DataFrame methods.

## 3. DataFrame Array Notation

```python
orders_df.select(orders_df["order_date"]).show()
```

This also clearly references a column from a specific DataFrame.

Useful for:

- joins
- dynamic column names
- avoiding dot notation limitations

## 4. Column Object

```python
from pyspark.sql.functions import col, column

orders_df.select(col("cust_id"), column("order_status")).show()
```

`col()` and `column()` create Column objects.

Use column objects when:

- filtering
- using column functions
- building expressions programmatically
- avoiding SQL string conditions

Example:

```python
orders_df.where(col("order_status").like("PENDING%")).show()
```

## 5. Column Expression

`expr()` lets me write SQL-style expressions inside DataFrame API.

```python
from pyspark.sql.functions import expr

orders_df.select(
    "order_id",
    "cust_id",
    expr("cust_id + 1 AS new_cust_id")
).show()
```

Use `expr()` when:

- writing SQL-like calculations
- using `CASE WHEN`
- creating derived columns
- applying functions as SQL strings

## All Column Styles Together

```python
from pyspark.sql.functions import col, column, expr

orders_df.select(
    "order_id",
    orders_df.order_date,
    orders_df["order_date"],
    column("cust_id"),
    col("cust_id"),
    expr("order_status")
).show()
```

This works, but in real code avoid mixing too many styles unless teaching/demoing.

## Filtering With Column Object

```python
orders_df.where(col("order_status").like("PENDING%")).show()
```

## Filtering With SQL String

```python
orders_df.where("order_status LIKE 'PENDING%'").show()
```

Both are valid.

Column object style is safer when building conditions programmatically.

SQL string style is readable for SQL users.

## Why So Many Column Access Methods?

Main reasons:

- convenience
- SQL compatibility
- programmatic transformations
- avoiding ambiguity during joins

## Column Ambiguity In Joins

Suppose two DataFrames have the same column name:

```text
orders_df.cust_id
customers_df.cust_id
```

If I write:

```python
joined_df.select("cust_id")
```

Spark may not know which `cust_id` I mean.

Better:

```python
joined_df.select(
    orders_df["cust_id"].alias("order_customer_id"),
    customers_df["cust_id"].alias("customer_id")
)
```

Or rename before joining.

## Aggregations

Aggregation means combining multiple rows into fewer rows.

Examples:

- total count
- total sales
- average price
- max transaction amount
- count distinct invoices

Types covered here:

1. Simple aggregations
2. Grouping aggregations
3. Windowing aggregations, covered in next file

## Simple Aggregations

Simple aggregation returns one output row.

Example business questions:

- How many records are there?
- How many unique invoices?
- What is total quantity?
- What is average unit price?

Dataset:

```text
/public/trendytech/datasets/order_data.csv
```

Read:

```python
orders_df = (
    spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("/public/trendytech/datasets/order_data.csv")
)
```

For production, use explicit schema instead of `inferSchema`.

## Simple Aggregation: Programmatic Style

```python
from pyspark.sql.functions import count, countDistinct, sum, avg

orders_df.select(
    count("*").alias("row_count"),
    countDistinct("InvoiceNo").alias("unique_invoice"),
    sum("Quantity").alias("total_quantity"),
    avg("UnitPrice").alias("avg_price")
).show()
```

Output shape:

```text
one row with row_count, unique_invoice, total_quantity, avg_price
```

## Simple Aggregation: `selectExpr`

```python
orders_df.selectExpr(
    "count(*) AS row_count",
    "count(distinct InvoiceNo) AS unique_invoice",
    "sum(Quantity) AS total_quantity",
    "avg(UnitPrice) AS avg_price"
).show()
```

This is more SQL-like.

## Simple Aggregation: Spark SQL

```python
orders_df.createOrReplaceTempView("orders")

spark.sql("""
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT InvoiceNo) AS unique_invoice,
        SUM(Quantity) AS total_quantity,
        AVG(UnitPrice) AS avg_price
    FROM orders
""").show()
```

## Which Style Should I Use?

| Style | Best For |
|---|---|
| Programmatic API | PySpark codebases, type-aware transformations |
| `selectExpr` | quick SQL-like expressions inside DataFrame API |
| Spark SQL | SQL-heavy logic, analysts, migration from Hive/SQL |

Performance is usually similar because Spark optimizes all through Catalyst.

Choose readability and maintainability.

## Grouping Aggregations

Grouping aggregation returns one row per group.

Example:

```text
group by country, invoice number
```

Business questions:

- total quantity per invoice per country
- invoice value per invoice per country

## Grouping Aggregation: Programmatic Style

```python
from pyspark.sql.functions import sum, expr

summary_df = (
    orders_df
    .groupBy("Country", "InvoiceNo")
    .agg(
        sum("Quantity").alias("total_quantity"),
        sum(expr("Quantity * UnitPrice")).alias("invoice_value")
    )
    .sort("InvoiceNo")
)

summary_df.show()
```

## Grouping Aggregation: Expression Style

```python
summary_df = (
    orders_df
    .groupBy("Country", "InvoiceNo")
    .agg(
        expr("sum(Quantity) AS total_quantity"),
        expr("sum(Quantity * UnitPrice) AS invoice_value")
    )
    .sort("InvoiceNo")
)

summary_df.show()
```

## Grouping Aggregation: Spark SQL

```python
orders_df.createOrReplaceTempView("orders")

spark.sql("""
    SELECT
        Country,
        InvoiceNo,
        SUM(Quantity) AS total_quantity,
        SUM(Quantity * UnitPrice) AS invoice_value
    FROM orders
    GROUP BY Country, InvoiceNo
    ORDER BY InvoiceNo
""").show()
```

## Important Aggregation Functions

Common functions:

```python
count()
countDistinct()
sum()
avg()
min()
max()
first()
last()
collect_list()
collect_set()
```

Examples:

```python
orders_df.groupBy("Country").agg(
    count("*").alias("row_count"),
    countDistinct("InvoiceNo").alias("invoice_count"),
    sum("Quantity").alias("total_quantity"),
    avg("UnitPrice").alias("avg_price")
).show()
```

## Null Behavior In Aggregations

Important:

- `count("*")` counts all rows.
- `count("column")` counts non-null values in that column.
- `sum`, `avg`, `min`, `max` ignore null values.
- If all values are null, result can be null.

Example:

```python
orders_df.select(
    count("*").alias("all_rows"),
    count("UnitPrice").alias("non_null_prices")
).show()
```

## Aggregation And Shuffle

`groupBy` usually causes shuffle.

Why?

Rows with the same group key must come together.

Example:

```text
Country = Germany
```

All Germany records may be spread across partitions.

Spark shuffles them so aggregation can happen correctly.

Performance tips:

- filter before groupBy
- select only needed columns
- avoid grouping on very high-cardinality columns if unnecessary
- watch for data skew
- tune shuffle partitions

## Common Mistakes

- Using `count(column)` when `count(*)` is required.
- Forgetting that groupBy causes shuffle.
- Using wrong case in column names like `invoiceno` vs `InvoiceNo`.
- Not aliasing aggregate columns.
- Calculating invoice value after aggregation incorrectly.
- Grouping by too many columns and creating huge result sets.

## Best Practices

- Use explicit schema for production.
- Alias all aggregated columns.
- Filter early.
- Select required columns before aggregation.
- Use Spark SQL for complex aggregation logic if it is easier to read.
- Check null behavior before trusting metrics.
- Use `explain()` for important queries.

## Interview Questions

### Beginner Questions

- What is aggregation?
- What is the difference between simple aggregation and grouping aggregation?
- What does `count("*")` do?
- What is `countDistinct`?
- How do you calculate average in PySpark?

### Intermediate Questions

- Why does `groupBy` cause shuffle?
- What is the difference between `count("*")` and `count("col")`?
- How do nulls affect aggregations?
- How do you write aggregation using Spark SQL?
- What is the difference between `select` and `selectExpr`?

### Senior Data Engineer Questions

- How would you optimize a large grouping aggregation?
- How would you handle skewed group keys?
- How do you validate aggregate metrics in production?
- When would you pre-aggregate data?
- How would you design aggregate tables for dashboards?

## Scenario-Based Questions

### Scenario 1: Aggregation Job Is Slow

Check:

- input data size
- group key cardinality
- shuffle size
- skewed keys
- unnecessary columns
- filter pushdown
- shuffle partitions

### Scenario 2: Count Looks Wrong

Check:

- `count("*")` vs `count(column)`
- null values
- duplicate rows
- filters
- joins before aggregation

## Quick Revision

- Columns can be accessed as strings, objects, expressions, or DataFrame-prefixed columns.
- Use DataFrame-prefixed columns to avoid join ambiguity.
- Simple aggregation returns one row.
- Grouping aggregation returns one row per group.
- `groupBy` usually causes shuffle.
- `count("*")` counts rows.
- `count(column)` counts non-null values.
