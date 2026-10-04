# Assignment Solutions And Practical Guide

## Introduction

This section provides practical solutions for the assignment topics:

- reading JSON users data
- checking partitions
- basic analysis
- writing Parquet output
- packaging code for `spark-submit`
- creating pivot output
- understanding initial partitions for multiple files
- cleanup

All code uses placeholders or `getpass.getuser()` instead of hardcoded lab usernames.

## SparkSession

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession
    .builder
    .appName("users-assignment")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

## Question 1: Read Users Data And Check Partitions

Path:

```text
/public/sms/users
```

The data is JSON and contains nested address and array of phone numbers.

Schema:

```python
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType
)

users_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("user_first_name", StringType(), nullable=False),
    StructField("user_last_name", StringType(), nullable=False),
    StructField("user_email", StringType(), nullable=False),
    StructField("user_gender", StringType(), nullable=False),
    StructField("user_phone_numbers", ArrayType(StringType()), nullable=True),
    StructField("user_address", StructType([
        StructField("street", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("state", StringType(), nullable=False),
        StructField("postal_code", StringType(), nullable=False),
    ]), nullable=False),
])
```

Read:

```python
users_df = (
    spark.read
    .format("json")
    .schema(users_schema)
    .load("/public/sms/users/")
)

print("Initial partitions:", users_df.rdd.getNumPartitions())
```

## Question 2: Basic Analysis

Flatten nested columns:

```python
from pyspark.sql.functions import col, size

users_flat_df = (
    users_df
    .withColumn("user_street", col("user_address.street"))
    .withColumn("user_city", col("user_address.city"))
    .withColumn("user_state", col("user_address.state"))
    .withColumn("user_postal_code", col("user_address.postal_code"))
    .withColumn("num_phone_numbers", size(col("user_phone_numbers")))
)

users_flat_df.createOrReplaceTempView("users_vw")
```

## 2a: Total Records

```python
total_records = users_df.count()
print("Total records:", total_records)
```

SQL:

```python
spark.sql("SELECT COUNT(*) AS total_records FROM users_vw").show()
```

## 2b: Users From New York

```python
spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE user_state = 'New York'
""").show()
```

## 2c: State With Maximum Postal Codes

```python
spark.sql("""
    SELECT
        user_state,
        COUNT(DISTINCT user_postal_code) AS postal_count
    FROM users_vw
    WHERE user_state IS NOT NULL
    GROUP BY user_state
    ORDER BY postal_count DESC
    LIMIT 1
""").show()
```

## 2d: City With Most Users

```python
spark.sql("""
    SELECT
        user_city,
        COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE user_city IS NOT NULL
    GROUP BY user_city
    ORDER BY user_count DESC
    LIMIT 1
""").show()
```

## 2e: Users With Email Domain `bizjournals.com`

```python
spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE user_email LIKE '%bizjournals.com'
""").show()
```

Better exact-domain approach:

```python
spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE lower(split(user_email, '@')[1]) = 'bizjournals.com'
""").show()
```

Why this is better:

`LIKE '%bizjournals.com'` can match unexpected strings.

Splitting by `@` checks the actual email domain.

## 2f: Users With 4 Phone Numbers

```python
spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE num_phone_numbers = 4
""").show()
```

## 2g: Users With No Phone Number

```python
spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS user_count
    FROM users_vw
    WHERE user_phone_numbers IS NULL
       OR size(user_phone_numbers) = 0
""").show()
```

Why include `size = 0`?

Some records may have an empty array instead of null.

## Question 3: Write Base DataFrame In Parquet Format

```python
output_path = f"/user/{username}/section09/assignment/users_parquet"

users_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", output_path) \
    .save()
```

Check files from terminal:

```bash
hadoop fs -ls -h /user/<username>/section09/assignment/users_parquet
```

Observation:

Number of output part files is related to number of DataFrame partitions.

## Question 4: Package Q1 And Q2 As Python File

Create `question4.py`.

Script outline:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, size
import getpass

def main():
    username = getpass.getuser()

    spark = (
        SparkSession.builder
        .appName("users-basic-analysis")
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
        .enableHiveSupport()
        .master("yarn")
        .getOrCreate()
    )

    users_schema = StructType([
        StructField("user_id", IntegerType(), nullable=False),
        StructField("user_first_name", StringType(), nullable=False),
        StructField("user_last_name", StringType(), nullable=False),
        StructField("user_email", StringType(), nullable=False),
        StructField("user_gender", StringType(), nullable=False),
        StructField("user_phone_numbers", ArrayType(StringType()), nullable=True),
        StructField("user_address", StructType([
            StructField("street", StringType(), nullable=False),
            StructField("city", StringType(), nullable=False),
            StructField("state", StringType(), nullable=False),
            StructField("postal_code", StringType(), nullable=False),
        ]), nullable=False),
    ])

    users_df = spark.read.schema(users_schema).json("/public/sms/users/")

    print("Initial partitions:", users_df.rdd.getNumPartitions())
    print("Total records:", users_df.count())

    users_flat_df = (
        users_df
        .withColumn("user_city", col("user_address.city"))
        .withColumn("user_state", col("user_address.state"))
        .withColumn("user_postal_code", col("user_address.postal_code"))
        .withColumn("num_phone_numbers", size(col("user_phone_numbers")))
    )

    users_flat_df.createOrReplaceTempView("users_vw")

    spark.sql("SELECT COUNT(DISTINCT user_id) AS new_york_users FROM users_vw WHERE user_state = 'New York'").show()
    spark.sql("SELECT user_state, COUNT(DISTINCT user_postal_code) AS postal_count FROM users_vw GROUP BY user_state ORDER BY postal_count DESC LIMIT 1").show()
    spark.sql("SELECT user_city, COUNT(DISTINCT user_id) AS user_count FROM users_vw WHERE user_city IS NOT NULL GROUP BY user_city ORDER BY user_count DESC LIMIT 1").show()
    spark.sql("SELECT COUNT(DISTINCT user_id) AS bizjournals_users FROM users_vw WHERE lower(split(user_email, '@')[1]) = 'bizjournals.com'").show()
    spark.sql("SELECT COUNT(DISTINCT user_id) AS four_phone_users FROM users_vw WHERE num_phone_numbers = 4").show()
    spark.sql("SELECT COUNT(DISTINCT user_id) AS no_phone_users FROM users_vw WHERE user_phone_numbers IS NULL OR size(user_phone_numbers) = 0").show()

    spark.stop()

if __name__ == "__main__":
    main()
```

Run in client mode:

```bash
spark-submit \
  --master yarn \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 4G \
  --conf spark.dynamicAllocation.enabled=false \
  question4.py
```

## Question 5: Pivot State And Gender

Requirement:

Rows:

```text
state
```

Columns:

```text
Male, Female
```

Aggregation:

Count records where phone number is not null.

Better pivot approach:

```python
pivot_df = (
    users_flat_df
    .filter("user_state IS NOT NULL")
    .filter("user_phone_numbers IS NOT NULL")
    .groupBy("user_state")
    .pivot("user_gender", ["Male", "Female"])
    .count()
    .orderBy("user_state")
)

pivot_df.show()
```

Why this is better than manual `CASE WHEN`:

- shorter
- clearly expresses pivot
- easier to maintain
- matches assignment requirement

## Question 6: Package Pivot And Run In Cluster Mode

Create `question6.py`.

Core output logic:

```python
output_path = f"/user/{username}/pivot_assignment_result"

pivot_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", output_path) \
    .save()
```

Cluster submit:

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --num-executors 4 \
  --executor-cores 1 \
  --executor-memory 2G \
  --driver-memory 2G \
  --driver-cores 1 \
  --conf spark.dynamicAllocation.enabled=false \
  --verbose \
  question6.py
```

Check output:

```bash
hadoop fs -ls /user/<username>/pivot_assignment_result
```

## Question 7: Airlines Initial Partitions

Read all files:

```python
airlines_df = (
    spark.read
    .format("csv")
    .load("/public/airlines_all/airlines/")
)

airlines_df.rdd.getNumPartitions()
```

Check configs:

```python
spark.conf.get("spark.sql.files.maxPartitionBytes")
spark.conf.get("spark.sql.files.openCostInBytes")
spark.sparkContext.defaultParallelism
```

Why partition count appears:

Spark groups input files into partitions using:

- max partition bytes, default around 128 MB
- open cost per file, default around 4 MB
- file sizes
- default parallelism

If files are around 64 MB:

```text
64 MB file + 4 MB open cost = 68 MB
```

Two files:

```text
68 + 68 = 136 MB
```

This exceeds 128 MB, so Spark cannot combine two such files into one partition.

If one file is 64 MB and another is 48 MB:

```text
(64 + 4) + (48 + 4) = 120 MB
```

This fits within 128 MB, so they can be combined.

## Change `maxPartitionBytes` To 140 MB

140 MB in bytes:

```text
146800640
```

Create new SparkSession:

```python
spark = (
    SparkSession.builder
    .appName("airlines-partition-demo")
    .config("spark.sql.files.maxPartitionBytes", "146800640")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)

airlines_df_changed = (
    spark.read
    .format("csv")
    .load("/public/airlines_all/airlines/")
)

airlines_df_changed.rdd.getNumPartitions()
```

Why partition count changes:

Two 64 MB files with open cost:

```text
68 + 68 = 136 MB
```

Now 136 MB fits within 140 MB.

So Spark can combine more files per partition.

Partition count reduces.

## Question 8: Cleanup

Remove assignment result:

```bash
hadoop fs -rm -R /user/<username>/pivot_assignment_result
```

Remove section output if created:

```bash
hadoop fs -rm -R /user/<username>/section09
```

Be careful:

Only delete directories you created.

## Common Mistakes

- Not flattening nested JSON before SQL analysis.
- Using `LIKE '%domain'` instead of exact domain parsing.
- Counting null phone arrays but missing empty arrays.
- Hardcoding usernames in scripts.
- Running cluster mode and expecting terminal output.
- Writing pivot result without creating a valid output path.
- Forgetting `spark.stop()`.
- Deleting wrong HDFS path during cleanup.

## Best Practices

- Define schema explicitly for JSON.
- Flatten nested fields into clear columns.
- Use `size()` for array length.
- Use pivot API for pivot requirements.
- Use `getpass.getuser()` for lab paths.
- Use Parquet for assignment output.
- Check partition count before/after config change.
- Document why partition count changed.

## Interview Questions

### Beginner Questions

- How do you read nested JSON in Spark?
- How do you count DataFrame partitions?
- How do you write a DataFrame in Parquet?
- How do you create a pivot in PySpark?
- How do you check array length?

### Intermediate Questions

- How do you flatten nested struct columns?
- Why should scripts avoid hardcoded usernames?
- How does `maxPartitionBytes` affect partition count?
- What is `openCostInBytes`?
- Why does cluster mode not show print output locally?

### Senior Data Engineer Questions

- How would you design a reusable assignment-style Spark job for production?
- How do nested JSON schemas affect performance?
- How would you optimize a high-volume user analytics pipeline?
- How would you manage output paths safely?
- How would you test spark-submit jobs before production scheduling?

## Quick Revision

- Read nested JSON with `StructType`.
- Use `df.rdd.getNumPartitions()` for partitions.
- Flatten nested structs using `col("parent.child")`.
- Use `size()` for arrays.
- Write analytics output in Parquet.
- Use pivot for state/gender cross-tab.
- Use spark-submit for packaged jobs.
- `maxPartitionBytes` changes initial partition planning.
