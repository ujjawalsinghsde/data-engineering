# Spark SQL Managed And External Tables

## Introduction

Spark SQL lets me create databases and tables on top of data in storage.

This is important because DataFrames are temporary, but Spark tables can be persistent.

Two main table types:

- Managed table.
- External table.

The biggest difference:

```text
Managed table: Spark owns data + metadata
External table: Spark owns metadata only
```

## Database In Spark SQL

Spark has a default database.

If I create a table without specifying database, it goes into `default`.

Create database:

```python
spark.sql("CREATE DATABASE IF NOT EXISTS retail")
```

Use database:

```python
spark.sql("USE retail")
```

Show databases:

```python
spark.sql("SHOW DATABASES").show()
```

Filter databases:

```python
spark.sql("SHOW DATABASES").filter("namespace LIKE 'retail%'").show()
```

Show tables:

```python
spark.sql("SHOW TABLES").show()
```

## Managed Table

Managed table is a table where Spark manages both metadata and data.

When managed table is dropped:

```text
metadata is deleted
data files are deleted
```

Data is stored under warehouse directory.

With this config:

```python
.config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
```

Managed table path is usually:

```text
/user/<username>/warehouse/<database_name>.db/<table_name>
```

## Create Managed Table

Example:

```python
db_name = f"{username}_retail"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")
```

Create table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING CSV
""")
```

Load data:

```python
orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/orders_wh/*")
)

orders_df.createOrReplaceTempView("orders_temp")

spark.sql(f"""
    INSERT INTO {db_name}.orders
    SELECT * FROM orders_temp
""")
```

Verify:

```python
spark.sql(f"SELECT * FROM {db_name}.orders LIMIT 5").show()
```

Describe:

```python
spark.sql(f"DESCRIBE EXTENDED {db_name}.orders").show(truncate=False)
```

Look for:

- Provider.
- Type.
- Location.
- Schema.

## External Table

External table is a table where Spark manages only metadata.

Data already exists in some location.

Spark just applies schema and table name on top of it.

When external table is dropped:

```text
metadata is deleted
data files remain
```

Use external table when:

- Data is shared by many users.
- Data lives in a common data lake path.
- Spark should not delete the underlying data.
- Table is created on top of raw/existing files.

## Create External Table

Example:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_ext (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING CSV
    OPTIONS (header='true')
    LOCATION '/public/ujjawalsingh/orders_wh'
""")
```

Verify:

```python
spark.sql(f"SELECT * FROM {db_name}.orders_ext LIMIT 5").show()
```

Describe:

```python
spark.sql(f"DESCRIBE EXTENDED {db_name}.orders_ext").show(truncate=False)
```

## Managed Vs External Table

| Feature | Managed Table | External Table |
|---|---|---|
| Spark owns metadata | Yes | Yes |
| Spark owns data | Yes | No |
| Data location | Warehouse directory | User-specified location |
| Drop table effect | Deletes metadata + data | Deletes metadata only |
| Best for | Spark-owned data | Shared/existing data |
| Production data lake | Less common for raw shared data | Common |

## Drop Behavior

Drop external:

```python
spark.sql(f"DROP TABLE IF EXISTS {db_name}.orders_ext")
```

Data remains at the external location.

Drop managed:

```python
spark.sql(f"DROP TABLE IF EXISTS {db_name}.orders")
```

Data under warehouse table path is deleted.

Important:

Before dropping tables, always know whether table is managed or external.

Check:

```python
spark.sql(f"DESCRIBE EXTENDED {db_name}.orders").show(truncate=False)
```

## Temporary View Vs Persistent Table

Temporary view:

```python
orders_df.createOrReplaceTempView("orders")
```

Persistent table:

```python
spark.sql(f"CREATE TABLE {db_name}.orders (...)")
```

Comparison:

| Feature | Temp View | Persistent Table |
|---|---|---|
| Lifetime | Spark session | Across sessions |
| Metadata | Temporary catalog | Metastore |
| Storage | Existing DataFrame plan | Storage path |
| Drop behavior | View removed | Depends table type |

## DML Operations In Open Source Spark

Common operations:

| Operation | Open Source Spark Table |
|---|---|
| SELECT | Works |
| INSERT | Works |
| UPDATE | Usually not supported for plain file tables |
| DELETE | Usually not supported for plain file tables |

In Databricks Delta Lake:

| Operation | Delta Table |
|---|---|
| SELECT | Works |
| INSERT | Works |
| UPDATE | Works |
| DELETE | Works |
| MERGE | Works |

Why:

Plain CSV/Parquet tables do not provide full transactional row-level operations by default. Delta Lake adds transaction log and ACID support.

## JSON Table Example

Read JSON:

```python
orders_json_df = spark.read.json(
    "/public/ujjawalsingh/orders_wh.json/part-00000-68544d18-9a34-443f-bf0e-1dd8103ff94e-c000.json"
)
```

Select columns in order:

```python
orders_json_df = orders_json_df.select(
    "order_id",
    "order_date",
    "customer_id",
    "order_status"
)
```

Create managed table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_json (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING JSON
""")

orders_json_df.createOrReplaceTempView("orders_json_temp")

spark.sql(f"""
    INSERT INTO {db_name}.orders_json
    SELECT * FROM orders_json_temp
""")
```

Create external table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.orders_json_ext (
        order_id INT,
        order_date STRING,
        customer_id INT,
        order_status STRING
    )
    USING JSON
    LOCATION '/public/ujjawalsingh/orders_wh.json'
""")
```

## Production Perspective

Managed tables are convenient for:

- Temporary/staging Spark-owned tables.
- Tables fully controlled by one pipeline.
- Learning and experiments.

External tables are better for:

- Raw data zones.
- Shared data lake locations.
- Multi-team access.
- Data that should not be deleted by dropping one table.

Modern lakehouse:

In Databricks/Delta, external tables are often created on top of cloud storage paths, with transaction support through Delta.

## Common Mistakes

- Dropping managed table without realizing data will be deleted.
- Creating external table on wrong path.
- Forgetting `OPTIONS (header='true')` for CSV with header.
- Thinking temp view is permanent.
- Creating tables in default database accidentally.
- Hardcoding lab username instead of using `username`.
- Assuming update/delete work on plain Spark CSV tables.

## Best Practices

- Use database names with your username in shared labs.
- Use external tables for shared data.
- Use managed tables for Spark-owned temporary data.
- Always run `DESCRIBE EXTENDED` before learning drop behavior.
- Prefer Parquet/Delta for production tables.
- Avoid raw CSV managed tables for production analytics.
- Document table location and ownership.

## Interview Questions

### Beginner Questions

- What is Spark SQL?
- What is a Spark database?
- What is a managed table?
- What is an external table?
- What is a metastore?

### Intermediate Questions

- What happens when managed table is dropped?
- What happens when external table is dropped?
- How do you check table location?
- What is difference between temp view and table?
- Which DML operations work in open-source Spark?

### Senior Data Engineer Questions

- When would you choose external table over managed table?
- How would you design tables in a shared data lake?
- Why does Delta Lake support update/delete but plain Parquet does not?
- How do table location and ownership affect governance?
- What risks exist when dropping production tables?

## Quick Revision

```text
Managed table = Spark owns data + metadata
External table = Spark owns metadata only

Drop managed = data + metadata deleted
Drop external = only metadata deleted

Temp view = session scoped
Spark table = persistent

Use DESCRIBE EXTENDED to check table type and location
```
