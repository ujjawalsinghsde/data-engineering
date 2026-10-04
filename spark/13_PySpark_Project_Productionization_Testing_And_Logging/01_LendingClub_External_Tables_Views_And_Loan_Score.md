# LendingClub External Tables, Views, And Loan Score

## 1. Cleaned Data Layer

After raw data is cleaned, the cleaned folder in the data lake contains domain-specific datasets:

```text
lendingclubproject/
  cleaned/
    customers_parquet/
    loans_parquet/
    loans_repayments_parquet/
    loans_defaulters_delinq_parquet/
    loans_defaulters_detail_records_enq_parquet/
```

These datasets are used by:

- analytics teams
- risk scoring teams
- reporting users
- data science pipelines

## 2. Why Create Tables On Cleaned Data?

DataFrames are session-scoped.

Tables are easier for downstream teams because they can use SQL.

A Spark/Hive table has:

```text
data + metadata
```

Data:

- stored in HDFS or data lake path

Metadata:

- stored in Hive metastore
- includes schema, table type, location, owner, and format

## 3. Managed Vs External Tables

### Managed Table

Spark owns both data and metadata.

If you drop a managed table:

```text
data is deleted
metadata is deleted
```

Use managed tables when:

- Spark owns the lifecycle of data
- data is derived and can be recreated
- table is internal to one application

### External Table

Spark owns only metadata.

If you drop an external table:

```text
metadata is deleted
data remains in the external location
```

Use external tables when:

- multiple teams access the same data
- data is stored in a shared lake path
- accidental table drop should not delete actual data

For this project, external tables are best because multiple teams consume cleaned lending data.

## 4. Create Database

Use a generic database name:

```python
database_name = "lending_club"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
```

In a multi-user lab or production environment, include a safe namespace:

```python
database_name = f"{username}_lending_club"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
```

## 5. Create Customers External Table

```python
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.customers (
    member_id string,
    emp_title string,
    emp_length int,
    home_ownership string,
    annual_income float,
    address_state string,
    address_zipcode string,
    address_country string,
    grade string,
    sub_grade string,
    verification_status string,
    total_high_credit_limit float,
    application_type string,
    joint_annual_income float,
    verification_status_joint string,
    ingest_date timestamp
)
STORED AS PARQUET
LOCATION '/public/trendytech/lendingclubproject/cleaned/customers_parquet'
""")
```

Validate:

```python
spark.sql(f"SELECT * FROM {database_name}.customers LIMIT 10").show()
```

## 6. Create Loans External Table

```python
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.loans (
    loan_id string,
    member_id string,
    loan_amount float,
    funded_amount float,
    loan_term_years int,
    interest_rate float,
    monthly_installment float,
    issue_date string,
    loan_status string,
    loan_purpose string,
    loan_title string,
    ingest_date timestamp
)
STORED AS PARQUET
LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_parquet'
""")
```

## 7. Create Repayments External Table

```python
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.loans_repayments (
    loan_id string,
    total_principal_received float,
    total_interest_received float,
    total_late_fee_received float,
    total_payment_received float,
    last_payment_amount float,
    last_payment_date string,
    next_payment_date string,
    ingest_date timestamp
)
STORED AS PARQUET
LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_repayments_parquet'
""")
```

## 8. Create Defaulter Tables

Delinquency table:

```python
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.loans_defaulters_delinq (
    member_id string,
    delinq_2yrs int,
    delinq_amnt float,
    mths_since_last_delinq int
)
STORED AS PARQUET
LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_defaulters_delinq_parquet'
""")
```

Public records, bankruptcies, and enquiries table:

```python
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.loans_defaulters_detail_rec_enq (
    member_id string,
    pub_rec int,
    pub_rec_bankruptcies int,
    inq_last_6mths int
)
STORED AS PARQUET
LOCATION '/public/trendytech/lendingclubproject/cleaned/loans_defaulters_detail_records_enq_parquet'
""")
```

## 9. View Vs Precomputed Table

Business requirement 1:

```text
Teams need a complete consolidated view of all cleaned datasets with latest data.
```

Solution:

Create a view.

Pros:

- always reads latest underlying table data
- quick to create
- no duplicate storage

Cons:

- query can be slow because joins run every time

Business requirement 2:

```text
Teams need very fast access and can accept data that is slightly older.
```

Solution:

Create a precomputed table with a scheduled job.

Pros:

- faster query response
- expensive joins run once on schedule

Cons:

- data can be stale
- requires extra storage

## 10. Create Consolidated View

```python
spark.sql(f"""
CREATE OR REPLACE VIEW {database_name}.customers_loan_v AS
SELECT
    l.loan_id,
    c.member_id,
    c.emp_title,
    c.emp_length,
    c.home_ownership,
    c.annual_income,
    c.address_state,
    c.address_zipcode,
    c.address_country,
    c.grade,
    c.sub_grade,
    c.verification_status,
    c.total_high_credit_limit,
    c.application_type,
    c.joint_annual_income,
    c.verification_status_joint,
    l.loan_amount,
    l.funded_amount,
    l.loan_term_years,
    l.interest_rate,
    l.monthly_installment,
    l.issue_date,
    l.loan_status,
    l.loan_purpose,
    r.total_principal_received,
    r.total_interest_received,
    r.total_late_fee_received,
    r.total_payment_received,
    r.last_payment_amount,
    r.last_payment_date,
    r.next_payment_date,
    d.delinq_2yrs,
    d.delinq_amnt,
    d.mths_since_last_delinq,
    e.pub_rec,
    e.pub_rec_bankruptcies,
    e.inq_last_6mths
FROM {database_name}.customers c
LEFT JOIN {database_name}.loans l
    ON c.member_id = l.member_id
LEFT JOIN {database_name}.loans_repayments r
    ON l.loan_id = r.loan_id
LEFT JOIN {database_name}.loans_defaulters_delinq d
    ON c.member_id = d.member_id
LEFT JOIN {database_name}.loans_defaulters_detail_rec_enq e
    ON c.member_id = e.member_id
""")
```

Query:

```python
spark.sql(f"SELECT * FROM {database_name}.customers_loan_v LIMIT 10").show()
```

## 11. Create Precomputed Table

```python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_name}.customers_loan_t AS
SELECT *
FROM {database_name}.customers_loan_v
""")
```

This table can be rebuilt weekly or daily depending on freshness requirement.

Example tradeoff:

```text
View:
latest data, slower query

Precomputed table:
faster query, data may be older
```

## 12. Loan Score Factors

Loan score is based on three major factors:

1. Loan repayment history
2. Loan defaulter history
3. Financial health

Weightage:

```text
Payment history       = 20%
Defaulter history     = 45%
Financial health      = 35%
```

Higher loan score means higher chance of loan approval.

## 13. Point Configurations

Store point values as Spark configs so rules are not hardcoded everywhere.

```python
spark.conf.set("spark.sql.unacceptable_rated_pts", 0)
spark.conf.set("spark.sql.very_bad_rated_pts", 100)
spark.conf.set("spark.sql.bad_rated_pts", 250)
spark.conf.set("spark.sql.good_rated_pts", 500)
spark.conf.set("spark.sql.very_good_rated_pts", 650)
spark.conf.set("spark.sql.excellent_rated_pts", 800)
```

Grade thresholds:

```python
spark.conf.set("spark.sql.unacceptable_grade_pts", 750)
spark.conf.set("spark.sql.very_bad_grade_pts", 1000)
spark.conf.set("spark.sql.bad_grade_pts", 1500)
spark.conf.set("spark.sql.good_grade_pts", 2000)
spark.conf.set("spark.sql.very_good_grade_pts", 2500)
```

## 14. Bad Member ID Handling

Repeated `member_id` values can create bad joins and duplicate scoring.

Find bad IDs in customers:

```python
bad_data_customer_df = spark.sql(f"""
SELECT member_id
FROM (
    SELECT member_id, count(*) AS total
    FROM {database_name}.customers
    GROUP BY member_id
    HAVING total > 1
)
""")
```

Find bad IDs in defaulter tables:

```python
bad_data_delinq_df = spark.sql(f"""
SELECT member_id
FROM (
    SELECT member_id, count(*) AS total
    FROM {database_name}.loans_defaulters_delinq
    GROUP BY member_id
    HAVING total > 1
)
""")

bad_data_records_enq_df = spark.sql(f"""
SELECT member_id
FROM (
    SELECT member_id, count(*) AS total
    FROM {database_name}.loans_defaulters_detail_rec_enq
    GROUP BY member_id
    HAVING total > 1
)
""")
```

Create consolidated bad member ID file:

```python
bad_customer_data_final_df = bad_data_customer_df.select("member_id") \
    .union(bad_data_delinq_df.select("member_id")) \
    .union(bad_data_records_enq_df.select("member_id")) \
    .distinct()

bad_customer_data_final_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/bad/bad_customer_data_final")
```

In production, this file should be sent to upstream teams for correction.

## 15. Payment History Points

Payment history depends on:

- last payment amount
- total payment received
- monthly installment
- funded amount

Example:

```python
payment_history_df = spark.sql(f"""
SELECT
    c.member_id,
    CASE
        WHEN r.last_payment_amount < (l.monthly_installment * 0.5)
            THEN ${{spark.sql.very_bad_rated_pts}}
        WHEN r.last_payment_amount >= (l.monthly_installment * 0.5)
             AND r.last_payment_amount < l.monthly_installment
            THEN ${{spark.sql.very_bad_rated_pts}}
        WHEN r.last_payment_amount = l.monthly_installment
            THEN ${{spark.sql.good_rated_pts}}
        WHEN r.last_payment_amount > l.monthly_installment
             AND r.last_payment_amount <= (l.monthly_installment * 1.5)
            THEN ${{spark.sql.very_good_rated_pts}}
        WHEN r.last_payment_amount > (l.monthly_installment * 1.5)
            THEN ${{spark.sql.excellent_rated_pts}}
        ELSE ${{spark.sql.unacceptable_rated_pts}}
    END AS last_payment_pts,
    CASE
        WHEN r.total_payment_received >= (l.funded_amount * 0.5)
            THEN ${{spark.sql.very_good_rated_pts}}
        WHEN r.total_payment_received < (l.funded_amount * 0.5)
             AND r.total_payment_received > 0
            THEN ${{spark.sql.good_rated_pts}}
        ELSE ${{spark.sql.unacceptable_rated_pts}}
    END AS total_payment_pts
FROM {database_name}.customers c
JOIN {database_name}.loans l
    ON c.member_id = l.member_id
JOIN {database_name}.loans_repayments r
    ON l.loan_id = r.loan_id
""")
```

## 16. Defaulter History Points

Defaulter history depends on:

- `delinq_2yrs`
- `pub_rec`
- `pub_rec_bankruptcies`
- `inq_last_6mths`

High delinquency, public records, bankruptcies, and recent enquiries should reduce score.

## 17. Financial Health Points

Financial health depends on:

- `home_ownership`
- `loan_status`
- `funded_amount`
- `total_high_credit_limit`
- `grade`

Examples:

- fully paid loan status gets higher points
- charged off status gets low points
- own home may get higher points than rent
- funded amount too close to high credit limit may reduce points

## 18. Final Loan Score

```python
loan_score_df = spark.sql("""
SELECT
    member_id,
    ((last_payment_pts + total_payment_pts) * 0.20) AS payment_history_pts,
    ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) AS defaulters_history_pts,
    ((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) AS financial_health_pts
FROM fh_ldh_ph_pts
""")

final_loan_score_df = loan_score_df.withColumn(
    "loan_score",
    loan_score_df.payment_history_pts +
    loan_score_df.defaulters_history_pts +
    loan_score_df.financial_health_pts
)
```

Assign final grade:

```python
final_loan_score_df.createOrReplaceTempView("loan_score_eval")

loan_score_final_df = spark.sql("""
SELECT
    ls.*,
    CASE
        WHEN loan_score > ${spark.sql.very_good_grade_pts} THEN 'A'
        WHEN loan_score <= ${spark.sql.very_good_grade_pts}
             AND loan_score > ${spark.sql.good_grade_pts} THEN 'B'
        WHEN loan_score <= ${spark.sql.good_grade_pts}
             AND loan_score > ${spark.sql.bad_grade_pts} THEN 'C'
        WHEN loan_score <= ${spark.sql.bad_grade_pts}
             AND loan_score > ${spark.sql.very_bad_grade_pts} THEN 'D'
        WHEN loan_score <= ${spark.sql.very_bad_grade_pts}
             AND loan_score > ${spark.sql.unacceptable_grade_pts} THEN 'E'
        WHEN loan_score <= ${spark.sql.unacceptable_grade_pts} THEN 'F'
    END AS loan_final_grade
FROM loan_score_eval ls
""")
```

Write processed score:

```python
loan_score_final_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save(f"/user/{username}/lendingclubproject/processed/loan_score")
```

## 19. Common Mistakes

1. Creating managed tables on shared cleaned data.
2. Dropping external tables and assuming data is deleted.
3. Joining duplicate `member_id` records without quarantine.
4. Hardcoding point values in many SQL strings.
5. Creating a view when business requires very fast repeated access.
6. Creating a precomputed table when business requires latest data.

## 20. Interview Questions

### Beginner

1. What is the difference between managed and external tables?
2. Why create external tables on cleaned data?
3. What is a Spark view?
4. What is a precomputed table?
5. What factors contribute to loan score?

### Intermediate

1. Why can repeated `member_id` values be bad data?
2. How do you consolidate bad IDs from multiple datasets?
3. When would you use a view vs table?
4. Why is Parquet used for cleaned tables?
5. How do you parameterize scoring thresholds?

### Senior

1. How would you design a scoring pipeline that supports changing business rules?
2. How would you explain freshness vs query performance tradeoff?
3. How would you validate loan score output?
4. How would you make this pipeline idempotent?
5. How would you handle slowly changing borrower attributes?

## 21. Quick Revision

- External tables are safer for shared cleaned data.
- Views give latest data but can be slow.
- Precomputed tables give faster access but can be stale.
- Loan score uses payment history, defaulter history, and financial health.
- Repeated member IDs should be quarantined before scoring.
