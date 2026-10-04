# Data Cleaning: Customers, Loans, Repayments, And Defaulters

## 1. Cleaning Strategy

The project has four cleaned datasets:

1. customers
2. loans
3. loan repayments
4. loan defaulters

Common cleaning steps:

- enforce schema
- rename columns
- add ingestion timestamp
- remove duplicates
- handle nulls
- convert datatypes
- standardize categorical values
- apply business rules
- write cleaned CSV and Parquet

## 2. Common Imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    regexp_replace,
    col,
    when,
    length,
    count
)
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

## 3. Cleaning Customers Data

### 3.1 Read With Explicit Schema

```python
customer_schema = """
member_id string,
emp_title string,
emp_length string,
home_ownership string,
annual_inc float,
addr_state string,
zip_code string,
country string,
grade string,
sub_grade string,
verification_status string,
tot_hi_cred_lim float,
application_type string,
annual_inc_joint float,
verification_status_joint string
"""

customers_raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(customer_schema) \
    .load("/public/trendytech/lendingclubproject/raw/customers_data_csv")
```

### 3.2 Rename Columns

```python
customers_renamed_df = customers_raw_df \
    .withColumnRenamed("annual_inc", "annual_income") \
    .withColumnRenamed("addr_state", "address_state") \
    .withColumnRenamed("zip_code", "address_zipcode") \
    .withColumnRenamed("country", "address_country") \
    .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
    .withColumnRenamed("annual_inc_joint", "joint_annual_income")
```

### 3.3 Add Ingestion Timestamp

```python
customers_ingested_df = customers_renamed_df.withColumn(
    "ingest_date",
    current_timestamp()
)
```

### 3.4 Remove Duplicate Rows

```python
customers_distinct_df = customers_ingested_df.distinct()
```

### 3.5 Remove Rows With Null Annual Income

Annual income is important for credit risk analysis. Rows without it may not be useful for downstream scoring.

```python
customers_income_df = customers_distinct_df.filter("annual_income is not null")
```

### 3.6 Clean Employment Length

Raw values may look like:

```text
10+ years
3 years
< 1 year
n/a
```

Remove non-digit characters:

```python
customers_emp_cleaned_df = customers_income_df.withColumn(
    "emp_length",
    regexp_replace(col("emp_length"), r"(\\D)", "")
)
```

Cast to integer:

```python
customers_emp_casted_df = customers_emp_cleaned_df.withColumn(
    "emp_length",
    col("emp_length").cast("int")
)
```

### 3.7 Fill Null Employment Length With Average

```python
customers_emp_casted_df.createOrReplaceTempView("customers")

avg_emp_length_row = spark.sql("""
SELECT floor(avg(emp_length)) AS avg_emp_length
FROM customers
""").collect()

avg_emp_duration = avg_emp_length_row[0]["avg_emp_length"]

customers_emp_filled_df = customers_emp_casted_df.na.fill(
    avg_emp_duration,
    subset=["emp_length"]
)
```

### 3.8 Clean State Code

State should be two characters. Replace invalid values with `NA`.

```python
customers_cleaned_df = customers_emp_filled_df.withColumn(
    "address_state",
    when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
)
```

### 3.9 Write Cleaned Customers

```python
customers_cleaned_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/customers_parquet")

customers_cleaned_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/customers_csv")
```

## 4. Cleaning Loans Data

### 4.1 Read With Explicit Schema

```python
loans_schema = """
loan_id string,
member_id string,
loan_amount float,
funded_amount float,
loan_term_months string,
interest_rate float,
monthly_installment float,
issue_date string,
loan_status string,
loan_purpose string,
loan_title string
"""

loans_raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(loans_schema) \
    .load("/public/trendytech/lendingclubproject/raw/loans_data_csv")
```

### 4.2 Add Ingestion Timestamp

```python
loans_ingested_df = loans_raw_df.withColumn("ingest_date", current_timestamp())
```

### 4.3 Drop Records With Critical Nulls

```python
loan_columns_to_check = [
    "loan_amount",
    "funded_amount",
    "loan_term_months",
    "interest_rate",
    "monthly_installment",
    "issue_date",
    "loan_status",
    "loan_purpose"
]

loans_filtered_df = loans_ingested_df.na.drop(subset=loan_columns_to_check)
```

### 4.4 Convert Loan Term From Months To Years

Raw value:

```text
36 months
60 months
```

Convert:

```python
loans_term_df = loans_filtered_df.withColumn(
    "loan_term_months",
    (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")
).withColumnRenamed("loan_term_months", "loan_term_years")
```

### 4.5 Standardize Loan Purpose

Allowed list:

```python
loan_purpose_lookup = [
    "debt_consolidation",
    "credit_card",
    "home_improvement",
    "other",
    "major_purchase",
    "medical",
    "small_business",
    "car",
    "vacation",
    "moving",
    "house",
    "wedding",
    "renewable_energy",
    "educational"
]
```

Replace unexpected values with `other`:

```python
loans_cleaned_df = loans_term_df.withColumn(
    "loan_purpose",
    when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose"))
    .otherwise("other")
)
```

### 4.6 Write Cleaned Loans

```python
loans_cleaned_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_parquet")

loans_cleaned_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_csv")
```

## 5. Cleaning Loan Repayments Data

### 5.1 Read With Explicit Schema

```python
loan_repay_schema = """
loan_id string,
total_principal_received float,
total_interest_received float,
total_late_fee_received float,
total_payment_received float,
last_payment_amount float,
last_payment_date string,
next_payment_date string
"""

loan_repay_raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(loan_repay_schema) \
    .load("/public/trendytech/lendingclubproject/raw/loans_repayments_csv")
```

### 5.2 Add Ingestion Timestamp

```python
loan_repay_ingested_df = loan_repay_raw_df.withColumn(
    "ingest_date",
    current_timestamp()
)
```

### 5.3 Drop Records With Critical Nulls

```python
repayment_columns_to_check = [
    "total_principal_received",
    "total_interest_received",
    "total_late_fee_received",
    "total_payment_received",
    "last_payment_amount"
]

loan_repay_filtered_df = loan_repay_ingested_df.na.drop(
    subset=repayment_columns_to_check
)
```

### 5.4 Fix Total Payment Received

If `total_payment_received` is zero but principal was received, calculate total payment as:

```text
principal + interest + late fee
```

```python
loan_repay_payment_fixed_df = loan_repay_filtered_df.withColumn(
    "total_payment_received",
    when(
        (col("total_payment_received") == 0.0) &
        (col("total_principal_received") != 0.0),
        col("total_principal_received") +
        col("total_interest_received") +
        col("total_late_fee_received")
    ).otherwise(col("total_payment_received"))
)
```

### 5.5 Remove Zero Payment Records

```python
loan_repay_nonzero_df = loan_repay_payment_fixed_df.filter(
    "total_payment_received != 0.0"
)
```

### 5.6 Replace Invalid Payment Dates

Payment dates should be valid date strings or null. A value like `0.0` is invalid.

```python
loan_repay_ldate_fixed_df = loan_repay_nonzero_df.withColumn(
    "last_payment_date",
    when(col("last_payment_date") == "0.0", None)
    .otherwise(col("last_payment_date"))
)

loan_repay_cleaned_df = loan_repay_ldate_fixed_df.withColumn(
    "next_payment_date",
    when(col("next_payment_date") == "0.0", None)
    .otherwise(col("next_payment_date"))
)
```

### 5.7 Write Cleaned Repayments

```python
loan_repay_cleaned_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_repayments_parquet")

loan_repay_cleaned_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_repayments_csv")
```

## 6. Cleaning Loan Defaulters Data

### 6.1 Read With Explicit Schema

```python
loan_defaulters_schema = """
member_id string,
delinq_2yrs float,
delinq_amnt float,
pub_rec float,
pub_rec_bankruptcies float,
inq_last_6mths float,
total_rec_late_fee float,
mths_since_last_delinq float,
mths_since_last_record float
"""

loan_def_raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(loan_defaulters_schema) \
    .load("/public/trendytech/lendingclubproject/raw/loans_defaulters_csv")
```

### 6.2 Convert Delinquency Years

```python
loan_def_processed_df = loan_def_raw_df.withColumn(
    "delinq_2yrs",
    col("delinq_2yrs").cast("integer")
).fillna(0, subset=["delinq_2yrs"])
```

### 6.3 Create Delinquency Dataset

Customers with missed or delayed payments:

```python
loan_def_processed_df.createOrReplaceTempView("loan_defaulters")

loan_def_delinq_df = spark.sql("""
SELECT
    member_id,
    delinq_2yrs,
    delinq_amnt,
    int(mths_since_last_delinq) AS mths_since_last_delinq
FROM loan_defaulters
WHERE delinq_2yrs > 0
   OR mths_since_last_delinq > 0
""")
```

### 6.4 Create Public Records And Enquiries Dataset

```python
loan_def_records_enq_df = spark.sql("""
SELECT member_id
FROM loan_defaulters
WHERE pub_rec > 0.0
   OR pub_rec_bankruptcies > 0.0
   OR inq_last_6mths > 0.0
""")
```

### 6.5 Write Cleaned Defaulter Outputs

```python
loan_def_delinq_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_defaulters_delinquency_parquet")

loan_def_records_enq_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_defaulters_records_enquiries_parquet")
```

Optional CSV:

```python
loan_def_delinq_df.write \
    .option("header", "true") \
    .format("csv") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/cleaned/loans_defaulters_delinquency_csv")
```

## 7. Data Quality Summary Table

| Dataset | Key Cleaning Steps |
|---|---|
| Customers | schema, rename columns, ingestion timestamp, duplicates, null income, employment length, state cleanup |
| Loans | schema, ingestion timestamp, critical null drops, loan term conversion, purpose standardization |
| Repayments | schema, ingestion timestamp, critical null drops, payment correction, zero payment removal, invalid date cleanup |
| Defaulters | schema, delinquency cast/fill, split delinquency and public-record/enquiry outputs |

## 8. Common Mistakes

1. Filling all nulls with zero without business approval.
2. Dropping records before checking how many are lost.
3. Converting dates without validating format.
4. Not preserving rejected/bad records separately.
5. Cleaning everything in one unreadable chain.
6. Writing only CSV for analytics.
7. Forgetting to add ingestion metadata.

## 9. Production Improvements

Recommended additions:

- write rejected records to quarantine path
- log before/after counts
- add data quality thresholds
- parameterize paths
- write as Parquet
- add unit tests for each cleaning rule
- avoid `collect()` for large data except small aggregations

## 10. Quick Revision

- Customers cleaning focuses on profile standardization.
- Loans cleaning focuses on loan term, purpose, and critical nulls.
- Repayment cleaning fixes inconsistent payment amounts and invalid dates.
- Defaulter cleaning creates focused delinquency and public-record datasets.
- Always write cleaned data to a separate cleaned layer.
