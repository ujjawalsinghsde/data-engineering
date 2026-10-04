# DataFrame Transformations And Deduplication

## Introduction

Once a DataFrame is created, most work happens through transformations.

Transformation means:

```text
Input DataFrame -> New DataFrame
```

DataFrames are immutable.

Immutable means we do not change the existing DataFrame in place.

Every transformation creates a new logical DataFrame.

Example:

```python
new_df = old_df.drop("some_column")
```

`old_df` still exists.

`new_df` is a new DataFrame with the column removed.

## Common Transformations

Important transformations:

- `withColumn`
- `withColumnRenamed`
- `drop`
- `select`
- `selectExpr`
- `distinct`
- `dropDuplicates`

These are daily-use operations for a Data Engineer.

## Load Order Items Example

Input fields:

```text
order_item_id, order_id, product_id, quantity, subtotal, product_price
```

Example rows:

```text
1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
```

Read data:

```python
raw_df = (
    spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/retail_db/order_items/part-00000")
)

refined_df = raw_df.toDF(
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "subtotal",
    "product_price"
)

refined_df.show()
```

In production, I should provide schema instead of using `inferSchema`.

For learning, this is okay.

## `withColumn`

`withColumn` is used to:

- add a new column
- modify an existing column

### Add New Column

```python
from pyspark.sql.functions import expr

df_with_total = refined_df.withColumn(
    "calculated_subtotal",
    expr("quantity * product_price")
)

df_with_total.show()
```

Expected result:

```text
new column calculated_subtotal is added
```

### Modify Existing Column

```python
updated_df = refined_df.withColumn(
    "product_price",
    expr("product_price * 1.2")
)
```

Here `product_price` already exists.

So Spark modifies/replaces that column in the new DataFrame.

Rule:

```text
column exists     -> withColumn modifies it
column not exists -> withColumn creates it
```

## `withColumnRenamed`

Used to rename a column.

Example:

```python
hospital_new_df = hospital_df.withColumnRenamed(
    "total_cost",
    "hospital_bill"
)
```

Use this for one or two column renames.

For renaming all columns, `toDF()` is cleaner:

```python
df = raw_df.toDF("col1", "col2", "col3")
```

## `drop`

Used to remove columns.

Drop one column:

```python
df1 = refined_df.drop("subtotal")
```

Drop multiple columns:

```python
df2 = train_df.drop("passenger_name", "age")
```

`drop` does not fail if the column does not exist in some Spark versions; this can hide mistakes.

So during development, check:

```python
df.printSchema()
```

## `select`

`select` is used to choose columns.

Example:

```python
refined_df.select("order_id", "product_id", "quantity").show()
```

If I want to use expressions, I need `expr`.

```python
from pyspark.sql.functions import expr

df1.select(
    "*",
    expr("product_price * quantity AS subtotal")
).show()
```

Why `expr`?

Because `"product_price * quantity AS subtotal"` is not a normal column name.

It is a SQL expression.

## `selectExpr`

`selectExpr` accepts SQL expressions directly as strings.

Example:

```python
df1.selectExpr(
    "*",
    "product_price * quantity AS subtotal"
).show()
```

This is cleaner when many expressions are SQL-like.

## `select` Vs `selectExpr`

| Feature | `select` | `selectExpr` |
|---|---|---|
| Normal column selection | Yes | Yes |
| SQL expression as string | Needs `expr()` | Direct |
| Column object support | Better | Not the main style |
| Readability | Explicit | Shorter for SQL expressions |
| Performance | Same | Same |

Example with `select`:

```python
df.select(
    "order_id",
    expr("quantity * product_price AS subtotal")
)
```

Example with `selectExpr`:

```python
df.selectExpr(
    "order_id",
    "quantity * product_price AS subtotal"
)
```

Use `select` when:

- using PySpark column functions
- mixing `col`, `lit`, `when`, `expr`
- I want explicit code

Use `selectExpr` when:

- expressions are mostly SQL
- quick transformation in notebook
- many derived columns are SQL-like

## `expr`

`expr` lets me write SQL expressions inside DataFrame code.

Example:

```python
from pyspark.sql.functions import expr

df = df.withColumn(
    "subtotal",
    expr("quantity * product_price")
)
```

`expr` is useful for:

- arithmetic
- `CASE WHEN`
- date functions
- string functions
- SQL-style logic

## Conditional Logic With `CASE WHEN`

Course example:

- Nike products: increase price by 20%
- Armour products: increase price by 10%
- other products: no change

```python
from pyspark.sql.functions import expr

new_df = df1.withColumn(
    "product_price",
    expr("""
        CASE
            WHEN product_name LIKE '%Nike%' THEN product_price * 1.2
            WHEN product_name LIKE '%Armour%' THEN product_price * 1.1
            ELSE product_price
        END
    """)
)

new_df.show()
```

This is common in real projects.

Examples:

- pricing rules
- risk classification
- customer segmentation
- status standardization
- business category mapping

## Alternative: `when` And `otherwise`

PySpark also has function-based conditional logic.

```python
from pyspark.sql.functions import col, when

new_df = df1.withColumn(
    "product_price",
    when(col("product_name").like("%Nike%"), col("product_price") * 1.2)
    .when(col("product_name").like("%Armour%"), col("product_price") * 1.1)
    .otherwise(col("product_price"))
)
```

Both are valid.

Use `CASE WHEN` when logic is easier in SQL.

Use `when` when writing pure PySpark style.

## Date Transformations

Hospital dataset example:

Fields:

```text
patient_id
admission_date
discharge_date
diagnosis
doctor_id
total_cost
```

Need to calculate duration of stay.

```python
from pyspark.sql.functions import expr

hospital_expr_df = hospital_new_df.withColumn(
    "duration_of_stay",
    expr("datediff(discharge_date, admission_date)")
)
```

`datediff(end_date, start_date)` returns number of days.

## Duplicate Removal

Duplicates are common in data pipelines.

Reasons:

- file loaded twice
- retry process created duplicate records
- upstream system sent same event again
- join created duplicate combinations
- source has poor data quality

Example data:

```python
mylist = [
    (1, "Kapil", 34),
    (1, "Kapil", 34),
    (1, "Satish", 26),
    (2, "Satish", 26),
]

df = spark.createDataFrame(mylist).toDF("id", "name", "age")
df.show()
```

## `distinct`

`distinct()` removes duplicate rows by considering all columns.

```python
df.distinct().show()
```

Input:

```text
1, Kapil, 34
1, Kapil, 34
1, Satish, 26
2, Satish, 26
```

Output:

```text
1, Kapil, 34
1, Satish, 26
2, Satish, 26
```

Only exact duplicate rows are removed.

## `dropDuplicates`

`dropDuplicates()` can remove duplicates based on all columns or selected columns.

All columns:

```python
df.dropDuplicates().show()
```

Subset of columns:

```python
df.dropDuplicates(["name", "age"]).show()
```

This keeps one row per unique `name, age` combination.

Example:

```text
1, Satish, 26
2, Satish, 26
```

These are duplicates if we only care about `name, age`.

They are not duplicates if we care about all columns.

## `distinct` Vs `dropDuplicates`

| Operation | Checks | Use Case |
|---|---|---|
| `distinct()` | all columns | remove exact duplicate rows |
| `dropDuplicates()` | all columns by default | same as distinct |
| `dropDuplicates(["col1", "col2"])` | selected columns | remove duplicates by business key |

Performance note:

Both can cause shuffle because Spark has to bring same keys/rows together.

Use subset columns whenever business logic allows it.

## Business Key Deduplication

In production, duplicates are often removed using business keys.

Example:

```python
deduped_orders = orders_df.dropDuplicates(["order_id"])
```

But be careful.

If same `order_id` has two different statuses, Spark keeps one arbitrary row.

Better pattern:

```text
order_id, update_timestamp
```

Keep the latest record per order.

This requires window functions, which come later in Spark learning.

## Transformation Chaining

Spark transformations can be chained.

```python
result_df = (
    refined_df
    .drop("subtotal")
    .withColumn("subtotal", expr("quantity * product_price"))
    .filter("subtotal > 100")
    .select("order_id", "product_id", "quantity", "subtotal")
)

result_df.show()
```

This style is clean and readable.

## Practical Example: Train Dataset

Tasks:

- Drop `passenger_name` and `age`
- Remove duplicates based on `train_number` and `ticket_number`
- Count unique train names

```python
train_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/datasets/train.csv")
)

dropped_df = train_df.drop("passenger_name", "age")

deduped_df = dropped_df.dropDuplicates(["train_number", "ticket_number"])

row_count = deduped_df.count()
unique_train_count = deduped_df.select("train_name").distinct().count()

print("Rows after deduplication:", row_count)
print("Unique train names:", unique_train_count)
```

Production improvement:

Define schema instead of using `inferSchema`.

## Practical Example: Hospital Dataset

Tasks:

1. Drop `doctor_id`
2. Rename `total_cost` to `hospital_bill`
3. Add `duration_of_stay`
4. Add `adjusted_total_cost`
5. Select final columns

Important date issue:

- `admission_date` format is `MM-dd-yyyy`
- `discharge_date` format is `yyyy-MM-dd`

If two date columns have different formats, reading both directly as `date` with one `dateFormat` is risky.

Better approach:

```python
from pyspark.sql.functions import to_date, expr

schema = """
patient_id integer,
admission_date string,
discharge_date string,
diagnosis string,
doctor_id integer,
total_cost float
"""

hosp_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load("/public/ujjawalsingh/datasets/hospital.csv")
)

hospital_final_df = (
    hosp_df
    .withColumn("admission_date", to_date("admission_date", "MM-dd-yyyy"))
    .withColumn("discharge_date", to_date("discharge_date", "yyyy-MM-dd"))
    .drop("doctor_id")
    .withColumnRenamed("total_cost", "hospital_bill")
    .withColumn("duration_of_stay", expr("datediff(discharge_date, admission_date)"))
    .withColumn(
        "adjusted_total_cost",
        expr("""
            CASE
                WHEN diagnosis = 'Heart Attack' THEN hospital_bill * 1.5
                WHEN diagnosis = 'Appendicitis' THEN hospital_bill * 1.2
                ELSE hospital_bill
            END
        """)
    )
    .select("patient_id", "diagnosis", "hospital_bill", "adjusted_total_cost")
)

hospital_final_df.show()
```

Why this approach is better:

- both date formats are handled explicitly
- raw parsing is easy to debug
- transformation chain is readable

## Common Mistakes

- Using `withColumn` repeatedly for many columns when `selectExpr` would be cleaner.
- Forgetting that `withColumn` replaces existing column if same name is used.
- Using `distinct()` when only key-based deduplication is required.
- Deduplicating without understanding business keys.
- Dropping columns too early before they are needed for validation.
- Using `LIKE` when exact equality is required.
- Using one `dateFormat` for multiple date columns with different formats.

## Best Practices

- Use `select` or `selectExpr` to keep only required columns.
- Use `withColumn` for one or two derived columns.
- Use `selectExpr` for many SQL-style derived columns.
- Deduplicate using business keys, not blindly on all columns.
- Validate counts before and after deduplication.
- Keep transformation chains readable.
- Avoid carrying unnecessary columns through the pipeline.
- Use `expr` for clear SQL logic, especially `CASE WHEN`.

## Performance Tips

- `distinct` and `dropDuplicates` can cause shuffle.
- Deduplicate after filtering unnecessary records.
- Deduplicate on fewer columns when business logic allows.
- Avoid many repeated `withColumn` calls in very wide DataFrames.
- Select only required columns before expensive operations.
- Use built-in functions instead of Python UDFs.

## Interview Questions

### Beginner Questions

- What does `withColumn` do?
- What is the difference between `drop` and `select`?
- What is the difference between `select` and `selectExpr`?
- How do you remove duplicate rows from a DataFrame?
- What does `withColumnRenamed` do?

### Intermediate Questions

- What is the difference between `distinct` and `dropDuplicates`?
- Why can deduplication be expensive?
- How do you write conditional logic in PySpark?
- What happens if `withColumn` uses an existing column name?
- How do you calculate date difference in Spark?

### Senior Data Engineer Questions

- How would you deduplicate streaming or daily incremental data?
- How do you decide the business key for deduplication?
- How would you preserve rejected duplicate records for audit?
- How would you optimize a DataFrame with 200 transformation columns?
- When would you use SQL expressions instead of PySpark functions?

## Scenario-Based Questions

### Scenario 1: Duplicate Orders In Reporting

Your dashboard shows duplicate orders.

Debug approach:

- Check raw source count.
- Check if files were loaded twice.
- Check join keys.
- Check if one-to-many join created duplicates.
- Identify business key.
- Use `dropDuplicates` or window logic.

Follow-up:

- Should duplicates be deleted or quarantined?
- Which record should be kept?

### Scenario 2: Price Update Logic Wrong

Nike prices are not updated correctly.

Check:

- product name case sensitivity
- null product names
- `LIKE` pattern
- column type of price
- whether column was overwritten correctly

## Real Project Perspective

Most production transformations are not glamorous.

They are usually:

- renaming columns
- converting types
- standardizing strings
- removing duplicates
- deriving business columns
- applying `CASE WHEN`
- selecting final columns

The quality of a Data Engineer often shows in how carefully these simple transformations are written and tested.

## Quick Revision

- `withColumn` adds or modifies.
- `withColumnRenamed` renames.
- `drop` removes columns.
- `select` chooses columns; expressions need `expr`.
- `selectExpr` accepts SQL expressions directly.
- `distinct` removes full-row duplicates.
- `dropDuplicates` can use selected columns.
- Deduplication can shuffle data.
- Always know the business key before removing duplicates.
