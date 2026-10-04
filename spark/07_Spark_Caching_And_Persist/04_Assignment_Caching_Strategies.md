# Assignment Caching Strategies

## Introduction

This section contains practical caching and persist strategies for two business scenarios:

1. Customer transaction analytics.
2. Hotel booking analytics.

The goal is not only to write queries, but to think like a Data Engineer:

- What data should be cached?
- When should it be cached?
- How do I measure improvement?
- When should cache be cleared?
- What should not be cached?

## SparkSession Setup

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession
    .builder
    .appName("spark-caching-assignment")
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

In the course lab, some notebooks use an extra config:

```python
.config("spark.shuffle.useOldFetchProtocol", "true")
```

Use it only if the lab environment needs it.

## Helper: Measure Time

For simple timing:

```python
import time

def measure_time(label, func):
    start = time.time()
    result = func()
    end = time.time()
    print(f"{label} took {end - start:.2f} seconds")
    return result
```

This is useful for learning.

For production tuning, Spark UI is more reliable.

## Question 1: Customer Transactions

Dataset:

```text
/public/trendytech/datasets/cust_transf.csv
```

Columns:

```text
customer_id
purchase_date
product_id
transaction_amount
```

## Load Customer Transactions

```python
cust_schema = """
customer_id long,
purchase_date date,
product_id integer,
transaction_amount double
"""

transactions_df = (
    spark.read
    .format("csv")
    .schema(cust_schema)
    .load("/public/trendytech/datasets/cust_transf.csv")
)

transactions_df.printSchema()
transactions_df.show(5)
```

## A.1 Top-Selling Products By Revenue

Business requirement:

Marketing team wants top-selling products by revenue for a date range.

Date range:

```python
start_date = "2023-05-01"
end_date = "2023-06-08"
```

The query will run frequently.

Caching strategy:

Cache the filtered date-range DataFrame, not the full raw dataset.

Why?

Because all repeated use cases use the same date range.

```python
filtered_df = transactions_df.filter(
    (transactions_df.purchase_date >= start_date) &
    (transactions_df.purchase_date <= end_date)
)
```

Without cache:

```python
def top_products_without_cache():
    return (
        filtered_df
        .groupBy("product_id")
        .sum("transaction_amount")
        .withColumnRenamed("sum(transaction_amount)", "revenue")
        .orderBy("revenue", ascending=False)
        .limit(10)
        .show()
    )

measure_time("Top products without cache", top_products_without_cache)
```

With cache:

```python
cached_filtered_df = filtered_df.cache()

# Materialize cache
cached_filtered_df.count()

def top_products_with_cache():
    return (
        cached_filtered_df
        .groupBy("product_id")
        .sum("transaction_amount")
        .withColumnRenamed("sum(transaction_amount)", "revenue")
        .orderBy("revenue", ascending=False)
        .limit(10)
        .show()
    )

measure_time("Top products with cache", top_products_with_cache)
```

Expected learning:

First cache materialization may take time.

Repeated queries on `cached_filtered_df` should be faster.

## A.2 Top 10 Customers By Transaction Amount

This uses the same date-range data.

So it should reuse `cached_filtered_df`.

```python
top_customers_df = (
    cached_filtered_df
    .groupBy("customer_id")
    .sum("transaction_amount")
    .withColumnRenamed("sum(transaction_amount)", "customer_amount")
    .orderBy("customer_amount", ascending=False)
    .limit(10)
)

top_customers_df.show()
```

Why this is good:

The expensive date filtering is already cached.

## A.3 Implement Using Spark External Table

Create database:

```python
db_name = f"{username}_cust_transaction"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
```

Create external table:

```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {db_name}.customer_transactions_ext (
        customer_id long,
        purchase_date date,
        product_id integer,
        transaction_amount double
    )
    USING csv
    LOCATION '/public/trendytech/datasets/cust_transf.csv'
""")
```

Top products before table cache:

```python
spark.sql(f"""
    SELECT product_id, SUM(transaction_amount) AS revenue
    FROM {db_name}.customer_transactions_ext
    WHERE purchase_date >= '2023-05-01'
      AND purchase_date <= '2023-06-08'
    GROUP BY product_id
    ORDER BY revenue DESC
    LIMIT 10
""").show()
```

Top customers before table cache:

```python
spark.sql(f"""
    SELECT customer_id, SUM(transaction_amount) AS customer_amount
    FROM {db_name}.customer_transactions_ext
    WHERE purchase_date >= '2023-05-01'
      AND purchase_date <= '2023-06-08'
    GROUP BY customer_id
    ORDER BY customer_amount DESC
    LIMIT 10
""").show()
```

Cache table:

```python
spark.sql(f"CACHE TABLE {db_name}.customer_transactions_ext")
```

Run the same queries again and compare time.

Better production strategy:

If only a date range is repeatedly queried, cache a filtered temp view instead of the entire table.

```python
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW customer_transactions_range AS
    SELECT *
    FROM {db_name}.customer_transactions_ext
    WHERE purchase_date >= '2023-05-01'
      AND purchase_date <= '2023-06-08'
""")

spark.sql("CACHE TABLE customer_transactions_range")
```

## A.4 Regular Customers Eligible For Offer

Requirement:

Find top 10 regular customers.

Definition:

At least one purchase in a month.

Create year/month columns:

```python
from pyspark.sql.functions import year, month, countDistinct

monthly_df = (
    transactions_df
    .withColumn("purchase_year", year("purchase_date"))
    .withColumn("purchase_month", month("purchase_date"))
)
```

Build monthly customer activity:

```python
customer_month_counts = (
    monthly_df
    .groupBy("customer_id", "purchase_year", "purchase_month")
    .agg(countDistinct("purchase_month").alias("distinct_months"))
)
```

Without persist:

```python
regular_customers = (
    customer_month_counts
    .filter("distinct_months = 1")
    .groupBy("customer_id")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
)

regular_customers.show()
```

With persist:

```python
customer_month_counts_persisted = customer_month_counts.persist()

# Materialize
customer_month_counts_persisted.count()

regular_customers = (
    customer_month_counts_persisted
    .filter("distinct_months = 1")
    .groupBy("customer_id")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
)

regular_customers.show()
```

Why persist here?

`customer_month_counts` is an aggregated intermediate result.

If many downstream reports use it, persisting avoids recomputation.

## A.5 Compare `cache()` Vs `persist(MEMORY_AND_DISK_SER)`

```python
from pyspark.storagelevel import StorageLevel

customer_month_counts_cache = customer_month_counts.cache()
measure_time("Materialize cache", lambda: customer_month_counts_cache.count())
measure_time("Regular customers from cache", lambda: (
    customer_month_counts_cache
    .filter("distinct_months = 1")
    .groupBy("customer_id")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
    .show()
))
customer_month_counts_cache.unpersist()
```

Persist serialized:

```python
customer_month_counts_ser = customer_month_counts.persist(
    StorageLevel.MEMORY_AND_DISK_SER
)

measure_time("Materialize MEMORY_AND_DISK_SER", lambda: customer_month_counts_ser.count())
measure_time("Regular customers from MEMORY_AND_DISK_SER", lambda: (
    customer_month_counts_ser
    .filter("distinct_months = 1")
    .groupBy("customer_id")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
    .show()
))
customer_month_counts_ser.unpersist()
```

What to compare in Spark UI:

- storage level
- memory used
- disk used
- cached partitions
- query time

## A.6 Compare Multiple Storage Levels

```python
from pyspark.storagelevel import StorageLevel

storage_levels = [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
    ("MEMORY_AND_DISK_SER", StorageLevel.MEMORY_AND_DISK_SER),
    ("DISK_ONLY", StorageLevel.DISK_ONLY),
]

for level_name, level in storage_levels:
    persisted_df = customer_month_counts.persist(level)

    measure_time(f"Materialize {level_name}", lambda: persisted_df.count())
    measure_time(f"Query using {level_name}", lambda: (
        persisted_df
        .filter("distinct_months = 1")
        .groupBy("customer_id")
        .count()
        .orderBy("count", ascending=False)
        .limit(10)
        .show()
    ))

    persisted_df.unpersist()
```

Important:

Run one storage level at a time.

Always unpersist before testing the next level.

## B. Customer Transaction History

Requirement:

Customer service frequently needs transaction history for a specific customer.

Simple function:

```python
def get_customer_history(customer_id):
    customer_history_df = (
        transactions_df
        .filter(transactions_df.customer_id == customer_id)
        .cache()
    )
    customer_history_df.count()
    return customer_history_df

customer_history_df = get_customer_history(1001)
customer_history_df.show()
```

Important improvement:

Do not cache a separate DataFrame for every random customer forever.

That can explode memory usage.

Better production options:

- partition data by customer/date if access pattern is common
- cache recent/high-value customers only
- use a serving database for point lookup
- use Delta/Parquet with partitioning and Z-ordering in lakehouse systems
- expire cached data after use

Clean up:

```python
customer_history_df.unpersist()
```

## C. Empty Cached DataFrame And Table

```python
cached_filtered_df.unpersist()
customer_month_counts_persisted.unpersist()
spark.sql(f"UNCACHE TABLE IF EXISTS {db_name}.customer_transactions_ext")
spark.catalog.clearCache()
```

## Question 2: Hotel Bookings External Table

Dataset:

```text
/public/trendytech/datasets/hotel_data.csv
```

Columns:

```text
booking_id
guest_name
checkin_date
checkout_date
room_type
total_price
```

## Create External Table

```python
hotel_db = f"{username}_hotel_usecase"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {hotel_db}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {hotel_db}.hotel_bookings_external (
        booking_id INT,
        guest_name STRING,
        checkin_date DATE,
        checkout_date DATE,
        room_type STRING,
        total_price DOUBLE
    )
    USING csv
    OPTIONS (header 'true')
    LOCATION '/public/trendytech/datasets/hotel_data.csv'
""")

spark.sql(f"""
    SELECT *
    FROM {hotel_db}.hotel_bookings_external
    LIMIT 5
""").show()
```

Note:

If the source CSV does not have a header, remove `OPTIONS (header 'true')`.

## 2A: Count Bookings Before And After Cache

Without cache:

```python
measure_time("Hotel booking count without cache", lambda: spark.sql(f"""
    SELECT COUNT(*)
    FROM {hotel_db}.hotel_bookings_external
""").show())
```

Cache table:

```python
spark.sql(f"CACHE TABLE {hotel_db}.hotel_bookings_external")
```

After cache:

```python
measure_time("Hotel booking count with cache", lambda: spark.sql(f"""
    SELECT COUNT(*)
    FROM {hotel_db}.hotel_bookings_external
""").show())
```

## 2B: Average Price By Room Type

Without caching:

```python
spark.sql(f"UNCACHE TABLE IF EXISTS {hotel_db}.hotel_bookings_external")

measure_time("Average price without cache", lambda: spark.sql(f"""
    SELECT room_type, AVG(total_price) AS avg_total_price
    FROM (
        SELECT *
        FROM {hotel_db}.hotel_bookings_external
        LIMIT 100
    ) t
    GROUP BY room_type
""").show())
```

With caching:

```python
spark.sql(f"CACHE TABLE {hotel_db}.hotel_bookings_external")

measure_time("Average price with cache", lambda: spark.sql(f"""
    SELECT room_type, AVG(total_price) AS avg_total_price
    FROM (
        SELECT *
        FROM {hotel_db}.hotel_bookings_external
        LIMIT 100
    ) t
    GROUP BY room_type
""").show())
```

## 2C: Uncache Table

```python
spark.sql(f"UNCACHE TABLE IF EXISTS {hotel_db}.hotel_bookings_external")
```

Or clear everything:

```python
spark.catalog.clearCache()
```

## Important Notes For Assignment

Small datasets may not show big performance improvement.

Reasons:

- read time is already low
- caching overhead may be similar to query time
- cluster load changes
- first run materializes cache

Performance gains are clearer when:

- data is large
- intermediate computation is expensive
- cached data is reused several times
- enough executor memory is available

## Common Mistakes

- Caching raw full dataset when only filtered subset is reused.
- Comparing first cached run with uncached run without considering cache materialization.
- Forgetting to unpersist between storage-level tests.
- Hardcoding lab usernames in database/table names.
- Running `CACHE TABLE` but not verifying in Spark UI.
- Not checking if CSV has header.
- Using cache for customer-specific lookup without cleanup.

## Best Practices

- Cache filtered date range for repeated business reports.
- Persist expensive aggregated intermediate DataFrames.
- Use user-specific database names.
- Measure before and after.
- Use Spark UI Storage tab.
- Clear cache after assignment.
- Prefer serving databases for frequent point lookups.
- Prefer Parquet/Delta for repeated analytics in production.

## Interview Questions

### Beginner Questions

- How do you cache a DataFrame?
- How do you cache a Spark table?
- How do you unpersist a DataFrame?
- What is `StorageLevel.MEMORY_AND_DISK`?
- Why do we materialize cache using `count()`?

### Intermediate Questions

- What DataFrame would you cache for repeated date-range analytics?
- How do you compare cache vs persist?
- Why might a small dataset not show cache improvement?
- How do you clear all cached tables?
- How would you cache a table used by Spark SQL queries?

### Senior Data Engineer Questions

- Design a caching strategy for a dashboard used by 200 analysts.
- How would you avoid cache memory pressure in a shared Spark cluster?
- How would you handle frequently requested customer histories?
- How do you benchmark storage levels correctly?
- When would you replace caching with better table design or serving storage?

## Scenario-Based Questions

### Scenario 1: Marketing Query Runs Frequently

Best approach:

- filter date range
- select required columns
- cache filtered subset
- run product/customer aggregations from cached subset
- unpersist after workflow

### Scenario 2: Customer Service Needs Fast Customer Lookup

Cache may help for a small number of repeated customers.

But for many random customers, better design is:

- partitioned table
- indexed serving database
- key-value store
- Delta table optimization

### Scenario 3: Persist Storage Test Gives Confusing Results

Check:

- Did you unpersist before next test?
- Did you materialize cache?
- Did dynamic allocation change executors?
- Is cluster busy?
- Are you comparing first run or second run?
- Is data too small?

## Real Project Perspective

Caching is a tactical optimization.

It should not replace good data modeling.

For repeated analytics, better long-term improvements may be:

- store curated data in Parquet/Delta
- partition by date
- optimize file sizes
- pre-aggregate common metrics
- use materialized views
- serve point lookups from low-latency databases

Cache is powerful, but it is temporary.

Good table design lasts longer.

## Quick Revision

- Cache the reused subset, not always the full raw data.
- Materialize cache before comparing.
- Use Spark UI to inspect cache.
- Persist expensive intermediate results.
- Test one storage level at a time.
- Unpersist after use.
- Table cache helps repeated SQL queries.
- Small datasets may not show big gains.
