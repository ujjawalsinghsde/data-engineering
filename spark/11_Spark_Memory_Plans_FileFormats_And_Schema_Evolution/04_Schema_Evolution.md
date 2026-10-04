# Schema Evolution

## 1. What Is Schema Evolution?

Schema evolution means handling changes in data structure over time.

Real-world data rarely stays fixed.

Examples:

- a new column is added
- an old column is removed
- a column is renamed
- a datatype changes
- column order changes
- nested fields are added

A data platform must handle these changes without breaking every pipeline.

## 2. Example Scenario

Initial orders data:

```text
order_id, order_date
1,2013-07-25
2,2013-07-25
```

Later, customer ID is added:

```text
order_id, order_date, customer_id
3,2013-07-25,12111
4,2013-07-25,8827
```

Later, order status is added:

```text
order_id, order_date, customer_id, order_status
5,2013-07-25,11318,COMPLETE
6,2013-07-25,7130,COMPLETE
```

This is common in ingestion pipelines.

## 3. Why Text Formats Struggle

CSV does not store schema inside the file.

If different files have different columns, Spark needs help to interpret them.

Problems:

- column order matters
- missing columns may shift data
- datatype changes are hard to detect
- no reliable embedded schema
- schema enforcement must be external

Example problem:

```text
orders3.csv:
order_id, order_date, customer_id, order_status

orders4.csv:
order_id, order_date, order_status, customer_id
```

Both have four columns, but the order is different. CSV alone cannot protect you.

## 4. Why Parquet/ORC/Avro Help

Specialized formats store schema metadata with data.

Benefits:

- schema can be read from files
- new columns can be merged
- missing columns can be filled with nulls
- column order is less fragile
- compatible changes are easier

Parquet and ORC are commonly used for analytics. Avro is commonly used when schema evolution is important in ingestion and streaming.

## 5. Schema Evolution With Parquet

Write first dataset:

```python
orders_schema_1 = "order_id long, order_date date"

orders_df_1 = spark.read \
    .format("csv") \
    .schema(orders_schema_1) \
    .load(f"/user/{username}/datasets/orders1.csv")

orders_df_1.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"/user/{username}/schema_evolution/orders_parquet")
```

Append second dataset with an extra column:

```python
orders_schema_2 = "order_id long, order_date date, customer_id long"

orders_df_2 = spark.read \
    .format("csv") \
    .schema(orders_schema_2) \
    .load(f"/user/{username}/datasets/orders2.csv")

orders_df_2.write \
    .mode("append") \
    .format("parquet") \
    .save(f"/user/{username}/schema_evolution/orders_parquet")
```

Read without schema merge:

```python
orders_df = spark.read \
    .format("parquet") \
    .load(f"/user/{username}/schema_evolution/orders_parquet")

orders_df.printSchema()
orders_df.show()
```

Depending on Spark behavior and discovered file ordering, Spark may not show the full merged schema.

Read with schema merge:

```python
orders_merged_df = spark.read \
    .format("parquet") \
    .option("mergeSchema", "true") \
    .load(f"/user/{username}/schema_evolution/orders_parquet")

orders_merged_df.printSchema()
orders_merged_df.show()
```

Missing values for old files become `NULL`.

## 6. Adding A Column

Adding a nullable column is usually a compatible schema evolution.

Old data:

```text
order_id, order_date
```

New data:

```text
order_id, order_date, customer_id
```

Merged result:

```text
order_id | order_date | customer_id
1        | 2013-07-25 | null
2        | 2013-07-25 | null
3        | 2013-07-25 | 12111
```

## 7. Dropping A Column

Dropping a column is more sensitive.

If new files no longer contain `customer_id`, merged reads can still include the column if old files have it.

New rows may show:

```text
customer_id = null
```

Production concern:

Downstream consumers may still expect the column. Dropping columns should be governed carefully.

## 8. Changing Datatype

Datatype changes are risky.

Example:

```text
customer_id long
customer_id string
```

This may cause:

- read failures
- null values
- schema merge conflicts
- incorrect downstream results

Safer approach:

1. Add a new column with the new type.
2. Backfill if needed.
3. Migrate downstream consumers.
4. Drop the old column later.

Example:

```text
customer_id long
customer_id_str string
```

## 9. Column Order Changes

Column order changes are dangerous in CSV.

With Parquet, fields are stored with names and metadata, so it is safer.

Still, do not rely on accidental behavior. Always define and validate schemas in production.

## 10. Schema Evolution Options

Read-time schema merge:

```python
spark.read \
    .option("mergeSchema", "true") \
    .parquet(path)
```

Session-level setting:

```python
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
```

Use session-level merge carefully. It can add overhead because Spark may need to inspect many file footers.

## 11. Performance Cost Of Schema Merge

Schema merge is useful, but it is not free.

Costs:

- reads metadata from multiple files
- can slow down planning
- can become expensive with many small files
- may hide uncontrolled schema drift

Best practice:

Use schema merge intentionally, not casually everywhere.

## 12. Schema Governance

In production, schema evolution should be controlled.

Good practices:

- maintain schema contracts
- validate incoming data
- use schema registry for streaming/Avro where appropriate
- version schemas
- test downstream compatibility
- avoid silent datatype changes
- monitor null rates after schema changes

## 13. Practical Schema Evolution Demo

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()

base_path = f"/user/{username}/schema_evolution/orders_parquet"
```

Write version 1:

```python
schema_v1 = "order_id long, order_date date"

df_v1 = spark.read \
    .format("csv") \
    .schema(schema_v1) \
    .load(f"/user/{username}/datasets/orders1.csv")

df_v1.write.mode("overwrite").parquet(base_path)
```

Append version 2:

```python
schema_v2 = "order_id long, order_date date, customer_id long"

df_v2 = spark.read \
    .format("csv") \
    .schema(schema_v2) \
    .load(f"/user/{username}/datasets/orders2.csv")

df_v2.write.mode("append").parquet(base_path)
```

Read merged:

```python
merged_df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(base_path)

merged_df.printSchema()
merged_df.show()
```

## 14. Common Mistakes

1. Treating CSV as schema-safe.
2. Changing datatype directly without migration.
3. Enabling schema merge globally without understanding cost.
4. Dropping columns without checking downstream jobs.
5. Ignoring column order changes in text files.
6. Not monitoring nulls after new columns are added.

## 15. Production Guidance

For data lake layers:

```text
Raw/Landing:
  preserve original data
  allow flexible ingestion
  store schema metadata if possible

Clean/Curated:
  enforce schemas
  validate data types
  standardize column names

Serving:
  stable schema contracts
  strong governance
```

Use schema evolution to support business change, not to allow uncontrolled data drift.

## 16. Interview Questions

### Beginner

1. What is schema evolution?
2. Which file formats support schema evolution?
3. What happens when a new column is added?
4. Why is CSV weak for schema evolution?
5. What is `mergeSchema`?

### Intermediate

1. What is the cost of schema merge?
2. How does Parquet handle missing columns?
3. Why are datatype changes risky?
4. How do you safely drop a column?
5. How do you handle column order changes?

### Senior

1. How would you design schema evolution for a data lake?
2. How do you prevent schema drift from breaking production?
3. When would you use Avro with schema registry?
4. How do you migrate a column from integer to string safely?
5. How do schema evolution and downstream contracts interact?

## 17. Quick Revision

- Schema evolution handles changing data structures.
- Adding nullable columns is usually safe.
- Dropping columns and changing datatypes are risky.
- Parquet/ORC/Avro store schema metadata.
- `mergeSchema` reads and merges schemas across files.
- Schema merge has planning overhead.
- Production systems need schema governance.
