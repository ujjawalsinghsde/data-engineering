# DataFrame And Spark SQL Use Cases

## Introduction

This note focuses on writing the same business logic using:

- DataFrame API.
- Spark SQL.

The course uses orders data to show that both APIs can solve the same problem.

DataFrame API is programmatic.

Spark SQL is SQL-style.

Both are optimized by Spark.

## Load Orders Data

```python
orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/public/ujjawalsingh/orders_wh/*")
)
```

Check:

```python
orders_df.show()
orders_df.printSchema()
```

Create temp view:

```python
orders_df.createOrReplaceTempView("orders")
```

Schema:

```text
order_id
order_date
customer_id
order_status
```

## Use Case 1: Top 15 Customers By Order Count

Business question:

Which customers placed the most orders?

### DataFrame API

```python
result = (
    orders_df
    .groupBy("customer_id")
    .count()
    .sort("count", ascending=False)
    .limit(15)
)

result.show()
```

### Spark SQL

```python
result = spark.sql("""
    SELECT customer_id, COUNT(order_id) AS count
    FROM orders
    GROUP BY customer_id
    ORDER BY count DESC
    LIMIT 15
""")

result.show()
```

Notes:

- `groupBy().count()` returns a DataFrame, so it is a transformation.
- `show()` is an action.

## Use Case 2: Orders Under Each Status

Business question:

How many orders are in each status?

### DataFrame API

```python
result = orders_df.groupBy("order_status").count()
result.show()
```

### Spark SQL

```python
result = spark.sql("""
    SELECT order_status, COUNT(order_id) AS count
    FROM orders
    GROUP BY order_status
""")

result.show()
```

Example statuses:

```text
COMPLETE
CLOSED
PENDING_PAYMENT
PROCESSING
CANCELED
```

## Use Case 3: Active Customers

Active customer means customer placed at least one order.

### DataFrame API

```python
active_count = orders_df.select("customer_id").distinct().count()
print(active_count)
```

Here:

- `select` is transformation.
- `distinct` is transformation.
- final `count()` is action.

### Spark SQL

```python
result = spark.sql("""
    SELECT COUNT(DISTINCT customer_id) AS active_customers
    FROM orders
""")

result.show()
```

## Use Case 4: Customers With Most CLOSED Orders

Business question:

Which customers have the most CLOSED orders?

### DataFrame API

```python
result = (
    orders_df
    .filter("order_status = 'CLOSED'")
    .groupBy("customer_id")
    .count()
    .sort("count", ascending=False)
)

result.show()
```

### Spark SQL

```python
result = spark.sql("""
    SELECT customer_id, COUNT(order_id) AS count
    FROM orders
    WHERE order_status = 'CLOSED'
    GROUP BY customer_id
    ORDER BY count DESC
""")

result.show()
```

## DataFrame Operations Cheat Sheet

### Select

```python
orders_df.select("order_id", "customer_id")
```

### Filter

```python
orders_df.filter("order_status = 'CLOSED'")
```

### Group And Count

```python
orders_df.groupBy("order_status").count()
```

### Sort

```python
orders_df.sort("count", ascending=False)
```

### Distinct

```python
orders_df.select("customer_id").distinct()
```

### Limit

```python
orders_df.limit(10)
```

### Show

```python
orders_df.show(10, truncate=False)
```

## Column Functions

Import:

```python
from pyspark.sql.functions import col, count, countDistinct, length, to_timestamp
```

Examples:

```python
orders_df.filter(col("customer_id") == 11599)
```

```python
orders_df.select(countDistinct("customer_id"))
```

```python
orders_df.withColumn("order_ts", to_timestamp("order_date"))
```

## Assignment Dataset: Products

Path:

```text
/public/ujjawalsingh/retail_db/products
```

Schema:

```text
product_id, category, product_name, description, price, image_url
```

Course note:

Products file may not have header, so the assignment adds header manually for demo.

Better production approach:

Define schema explicitly while reading.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("image_url", StringType(), True),
])

products_df = (
    spark.read
    .format("csv")
    .schema(products_schema)
    .load("/public/ujjawalsingh/retail_db/products")
)
```

Create view:

```python
products_df.createOrReplaceTempView("products")
```

### 2.1 Total Products

DataFrame:

```python
products_df.count()
```

SQL:

```python
spark.sql("SELECT COUNT(*) AS total_products FROM products").show()
```

### 2.2 Unique Categories

DataFrame:

```python
products_df.select("category").distinct().count()
```

SQL:

```python
spark.sql("""
    SELECT COUNT(DISTINCT category) AS unique_categories
    FROM products
""").show()
```

### 2.3 Top 5 Most Expensive Products

DataFrame:

```python
top5 = (
    products_df
    .select("product_name", "category", "price", "image_url")
    .orderBy("price", ascending=False)
    .limit(5)
)

top5.show(truncate=False)
```

SQL:

```python
spark.sql("""
    SELECT product_name, category, price, image_url
    FROM products
    ORDER BY price DESC
    LIMIT 5
""").show(truncate=False)
```

### 2.4 Products Over 100 By Category

DataFrame:

```python
products_above_100 = (
    products_df
    .filter("price > 100")
    .groupBy("category")
    .count()
    .withColumnRenamed("count", "number_of_products")
)

products_above_100.show()
```

SQL:

```python
spark.sql("""
    SELECT category, COUNT(*) AS product_count
    FROM products
    WHERE price > 100
    GROUP BY category
""").show()
```

### 2.5 Products Over 200 In Category 5

DataFrame:

```python
result = (
    products_df
    .filter("price > 200 AND category = 5")
    .select("product_name", "price")
)

result.show(truncate=False)
```

SQL:

```python
spark.sql("""
    SELECT product_name, price
    FROM products
    WHERE price > 200 AND category = 5
""").show(truncate=False)
```

## Assignment Dataset: Customers

Path:

```text
/public/ujjawalsingh/retail_db/customers
```

Schema:

```text
cust_id, cust_fname, cust_lname, cust_email, cust_password,
cust_street, cust_city, cust_state, cust_zipcode
```

Read with schema:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customers_schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("cust_fname", StringType(), True),
    StructField("cust_lname", StringType(), True),
    StructField("cust_email", StringType(), True),
    StructField("cust_password", StringType(), True),
    StructField("cust_street", StringType(), True),
    StructField("cust_city", StringType(), True),
    StructField("cust_state", StringType(), True),
    StructField("cust_zipcode", StringType(), True),
])

cust_df = (
    spark.read
    .format("csv")
    .schema(customers_schema)
    .load("/public/ujjawalsingh/retail_db/customers")
)
```

Create view:

```python
cust_df.createOrReplaceTempView("customers")
```

### 3.1 Customers In Each State

DataFrame:

```python
cust_df.groupBy("cust_state").count().orderBy("cust_state").show()
```

SQL:

```python
spark.sql("""
    SELECT cust_state, COUNT(*) AS customer_count
    FROM customers
    GROUP BY cust_state
    ORDER BY cust_state
""").show()
```

### 3.2 Top 5 Most Common Last Names

DataFrame:

```python
cust_df.groupBy("cust_lname").count().orderBy("count", ascending=False).limit(5).show()
```

SQL:

```python
spark.sql("""
    SELECT cust_lname, COUNT(*) AS last_name_count
    FROM customers
    GROUP BY cust_lname
    ORDER BY last_name_count DESC
    LIMIT 5
""").show()
```

### 3.3 Invalid Zip Codes

Valid zip code means length is 5.

DataFrame:

```python
from pyspark.sql.functions import length

invalid_zips = cust_df.filter(length("cust_zipcode") != 5)
invalid_zips.show()
```

SQL:

```python
spark.sql("""
    SELECT *
    FROM customers
    WHERE LENGTH(cust_zipcode) != 5
""").show()
```

### 3.4 Count Valid Zip Codes

DataFrame:

```python
valid_zip_count = cust_df.filter(length("cust_zipcode") == 5).count()
print(valid_zip_count)
```

SQL:

```python
spark.sql("""
    SELECT COUNT(*) AS valid_zipcodes_count
    FROM customers
    WHERE LENGTH(cust_zipcode) = 5
""").show()
```

### 3.5 Customers From Each City In California

DataFrame:

```python
ca_customer_counts = (
    cust_df
    .filter("cust_state = 'CA'")
    .groupBy("cust_city")
    .count()
)

ca_customer_counts.show()
```

SQL:

```python
spark.sql("""
    SELECT cust_city, COUNT(*) AS customer_count
    FROM customers
    WHERE cust_state = 'CA'
    GROUP BY cust_city
""").show()
```

## Common Mistakes

- Using `==` inside SQL string instead of `=`.
- Forgetting to create temp view before SQL.
- Reading no-header file with `header=true`.
- Letting Spark infer wrong column types.
- Calling `print(ca_customer_counts)` instead of `.show()`.
- Not using `truncate=False` for long product names or image URLs.

## Best Practices

- Use explicit schema for no-header files.
- Use DataFrame API and SQL side by side for learning.
- Convert to temp view for SQL queries.
- Use `show(truncate=False)` for wide text columns.
- Use `orderBy(...).limit(...)` for top-N queries.
- Use `countDistinct` or SQL `COUNT(DISTINCT ...)` for unique counts.

## Interview Questions

### Beginner Questions

- How do you count records in DataFrame?
- How do you group by a column?
- How do you create a temp view?
- How do you run SQL on DataFrame?
- How do you filter records?

### Intermediate Questions

- What is the difference between `count()` action and `groupBy().count()`?
- How do you find distinct count?
- How do you validate zip code length?
- How do you solve the same use case in DataFrame API and SQL?
- Why define schema explicitly?

### Senior Data Engineer Questions

- How would you productionize these assignment queries?
- How do you handle malformed CSV rows?
- How do you test DataFrame transformations?
- How would you optimize top-N queries?
- When would you choose SQL over DataFrame API?

## Quick Revision

```text
DataFrame API:
df.groupBy("col").count()
df.filter("condition")
df.select("col")
df.orderBy("col", ascending=False)

Spark SQL:
df.createOrReplaceTempView("table")
spark.sql("SELECT ... FROM table")

Action:
df.count(), df.show(), df.collect()

Transformation:
filter, select, groupBy().count(), orderBy, distinct
```
