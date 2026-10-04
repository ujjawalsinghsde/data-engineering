# LendingClub Project Architecture And Dataset Preparation

## 1. Project Objective

The objective is to build a cleaned lending data layer from a large raw loan dataset.

The cleaned outputs can be used by:

- risk analytics teams
- reporting teams
- data science teams
- credit scoring pipelines
- business intelligence dashboards

## 2. High-Level Architecture

```text
Raw LendingClub CSV
        |
        | PySpark
        v
Derived raw domain datasets
        |
        +-- customers_data
        +-- loans_data
        +-- loan_repayments
        +-- loan_defaulters
        |
        | PySpark cleaning
        v
Cleaned zone
        |
        +-- cleaned customers
        +-- cleaned loans
        +-- cleaned repayments
        +-- cleaned defaulters
        |
        v
Analytics / Risk scoring / Reporting
```

## 3. Layering Pattern

A realistic lake structure:

```text
lendingclub/
  raw/
    customers/
    loans/
    repayments/
    defaulters/
  cleaned/
    customers/
    loans/
    repayments/
    defaulters/
  processed/
    risk_scores/
    borrower_summary/
```

Raw layer:

- stores data close to source
- minimal transformations
- used for replay/reprocessing

Cleaned layer:

- schema enforced
- duplicates removed
- nulls handled
- datatypes standardized

Processed layer:

- business metrics
- risk indicators
- analytical tables

## 4. Spark Session

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .config("spark.ui.port", "0") \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .config("spark.shuffle.useOldFetchProtocol", "true") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()
```

## 5. Read Raw Dataset

For quick exploration:

```python
raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/public/trendytech/datasets/accepted_2007_to_2018Q4.csv")
```

In production, avoid `inferSchema` for repeated pipelines. Define schema explicitly where possible.

Create a view:

```python
raw_df.createOrReplaceTempView("lending_club_data")
```

## 6. Create A Stable Member ID

If `member_id` is missing or unreliable, generate a hash-based ID.

```python
from pyspark.sql.functions import sha2, concat_ws

member_id_columns = [
    "emp_title",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "zip_code",
    "addr_state",
    "grade",
    "sub_grade",
    "verification_status"
]

new_df = raw_df.withColumn(
    "member_id",
    sha2(concat_ws("||", *member_id_columns), 256)
)
```

Check row count and unique members:

```python
new_df.createOrReplaceTempView("new_table")

spark.sql("select count(*) as total_records from new_table").show()
spark.sql("select count(distinct member_id) as unique_members from new_table").show()
```

Find possible duplicate borrower profiles:

```python
spark.sql("""
SELECT member_id, count(*) AS total_count
FROM new_table
GROUP BY member_id
HAVING total_count > 1
ORDER BY total_count DESC
""").show()
```

## 7. Create Customers Dataset

Columns:

```text
member_id
emp_title
emp_length
home_ownership
annual_inc
addr_state
zip_code
country
grade
sub_grade
verification_status
tot_hi_cred_lim
application_type
annual_inc_joint
verification_status_joint
```

Code:

```python
customers_data_df = spark.sql("""
SELECT
    member_id,
    emp_title,
    emp_length,
    home_ownership,
    annual_inc,
    addr_state,
    zip_code,
    'USA' AS country,
    grade,
    sub_grade,
    verification_status,
    tot_hi_cred_lim,
    application_type,
    annual_inc_joint,
    verification_status_joint
FROM new_table
""")
```

Write:

```python
customers_data_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/raw/customers_data_csv")
```

## 8. Create Loans Dataset

Columns:

```text
loan_id
member_id
loan_amnt
funded_amnt
term
int_rate
installment
issue_d
loan_status
purpose
title
```

Code:

```python
loans_data_df = spark.sql("""
SELECT
    id AS loan_id,
    member_id,
    loan_amnt,
    funded_amnt,
    term,
    int_rate,
    installment,
    issue_d,
    loan_status,
    purpose,
    title
FROM new_table
""")
```

Write:

```python
loans_data_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/raw/loans_data_csv")
```

## 9. Create Loan Repayments Dataset

Columns:

```text
loan_id
total_rec_prncp
total_rec_int
total_rec_late_fee
total_pymnt
last_pymnt_amnt
last_pymnt_d
next_pymnt_d
```

Code:

```python
loan_repayments_df = spark.sql("""
SELECT
    id AS loan_id,
    total_rec_prncp,
    total_rec_int,
    total_rec_late_fee,
    total_pymnt,
    last_pymnt_amnt,
    last_pymnt_d,
    next_pymnt_d
FROM new_table
""")
```

Write:

```python
loan_repayments_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/raw/loans_repayments_csv")
```

## 10. Create Loan Defaulters Dataset

Columns:

```text
member_id
delinq_2yrs
delinq_amnt
pub_rec
pub_rec_bankruptcies
inq_last_6mths
total_rec_late_fee
mths_since_last_delinq
mths_since_last_record
```

Code:

```python
loan_defaulters_df = spark.sql("""
SELECT
    member_id,
    delinq_2yrs,
    delinq_amnt,
    pub_rec,
    pub_rec_bankruptcies,
    inq_last_6mths,
    total_rec_late_fee,
    mths_since_last_delinq,
    mths_since_last_record
FROM new_table
""")
```

Write:

```python
loan_defaulters_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/raw/loans_defaulters_csv")
```

## 11. About `repartition(1)`

The reference notebooks use `repartition(1)` to create one output file for learning/demo purposes.

In production, avoid using `repartition(1)` on large datasets because:

- it reduces parallelism
- one executor must handle all output
- it can cause memory pressure
- it creates a bottleneck

Better production options:

- allow Spark to write multiple part files
- use partitioning by business columns
- compact files later if needed
- use optimized table formats in lakehouse systems

## 12. Data Quality Checks After Dataset Split

Recommended checks:

```python
print("Customers:", customers_data_df.count())
print("Loans:", loans_data_df.count())
print("Repayments:", loan_repayments_df.count())
print("Defaulters:", loan_defaulters_df.count())
```

Null key checks:

```python
customers_data_df.filter("member_id is null").count()
loans_data_df.filter("loan_id is null").count()
loan_repayments_df.filter("loan_id is null").count()
loan_defaulters_df.filter("member_id is null").count()
```

Duplicate checks:

```python
customers_data_df.groupBy("member_id").count().filter("count > 1").show()
loans_data_df.groupBy("loan_id").count().filter("count > 1").show()
```

## 13. Recommended Output Formats

For learning:

- CSV is okay because it is easy to inspect.

For production:

- Parquet is preferred for Spark analytics.

Recommended:

```python
customers_data_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/raw/customers_data_parquet")
```

## 14. Common Mistakes

1. Using `inferSchema` in production jobs.
2. Writing one large output file with `repartition(1)`.
3. Not validating generated IDs.
4. Not checking duplicate counts.
5. Mixing raw and cleaned data in the same folder.
6. Hardcoding user-specific paths.
7. Not explaining why the dataset was split.

## 15. Interview Explanation

Sample:

```text
The raw lending dataset had more than 100 columns, so we split it into domain-specific datasets: customers, loans, repayments, and defaulters. This improved maintainability and made downstream analytics easier. Since the original member identifier was not reliable, we generated a deterministic SHA-2 based member ID from borrower profile attributes. After creating raw domain datasets, we applied schema enforcement and cleaning rules separately for each dataset and wrote the curated outputs in Parquet for efficient Spark consumption.
```

## 16. Quick Revision

- Split raw data into domain datasets.
- Generate stable `member_id` using `sha2` and `concat_ws`.
- Keep raw and cleaned layers separate.
- Avoid `repartition(1)` in production.
- Validate counts, nulls, and duplicates after each stage.
