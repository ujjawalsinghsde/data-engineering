# Project Interview Q&A And Production Checklist

## 1. One-Minute Project Summary

```text
I worked on a lending risk analytics data engineering project. The source was a large LendingClub-style loan dataset containing borrower profile, loan, repayment, and delinquency details. We used PySpark to split the raw file into domain datasets, generated a stable member identifier using SHA-2, enforced schemas, cleaned duplicates and nulls, standardized employment length, loan term, state code, payment amounts, and defaulter indicators, and wrote cleaned data in Parquet for analytics and risk scoring.
```

## 2. Architecture Explanation

```text
The architecture followed a lake-based pattern. Source data was ingested into the raw layer. PySpark jobs created domain-specific raw datasets such as customers, loans, repayments, and defaulters. Cleaning jobs then enforced schemas, applied data quality rules, and wrote curated data to the cleaned layer. The analytics and risk teams consumed the cleaned Parquet datasets for reporting and model feature creation.
```

## 3. Why Spark Was Used

Use this answer:

```text
The dataset was large and contained millions of records with many columns. We needed distributed processing for cleaning, transformations, joins, aggregations, and writing curated outputs. Running these analytical transformations on an operational database would have increased load on transactional systems. Spark allowed us to process the data in parallel and write optimized outputs like Parquet.
```

## 4. Your Role

Example:

```text
My role was to develop PySpark cleaning pipelines for borrower, loan, repayment, and defaulter datasets. I worked on schema enforcement, null handling, duplicate removal, datatype conversion, data quality validation, writing curated outputs, and performance checks using Spark UI. I also participated in sprint planning, code reviews, and deployment activities.
```

## 5. Cleaning Rules To Mention

Customers:

- renamed business columns
- added ingestion timestamp
- removed duplicate rows
- removed null annual income
- converted employment length to integer
- filled missing employment length with average
- standardized state codes

Loans:

- enforced schema
- dropped rows with critical nulls
- converted term from months to years
- standardized loan purpose

Repayments:

- enforced schema
- dropped critical nulls
- recalculated total payment when principal existed
- removed zero-payment records
- fixed invalid payment date values

Defaulters:

- converted delinquency years to integer
- filled null delinquency years with zero
- separated delinquency records
- separated public record and enquiry records

## 6. Transformations Used

Mention:

- `select`
- `withColumn`
- `withColumnRenamed`
- `distinct`
- `filter`
- `na.drop`
- `na.fill`
- `regexp_replace`
- `cast`
- `when`
- `length`
- `sha2`
- `concat_ws`
- `groupBy`
- Spark SQL temp views
- DataFrame writer API

## 7. Performance Considerations

Explain:

- avoided repeated raw reads where possible
- wrote cleaned data in Parquet for efficient reads
- used explicit schemas instead of schema inference
- checked partition counts and Spark UI
- avoided unnecessary columns in downstream datasets
- used CSV only for easy inspection, Parquet for analytics
- avoided `repartition(1)` in production-scale jobs

## 8. Unit Testing Strategy

Test cleaning functions with small DataFrames.

Examples:

- employment length cleanup
- loan term conversion
- invalid state replacement
- null annual income filtering
- loan purpose standardization
- total payment correction

Example:

```python
def test_loan_purpose_standardization(spark):
    data = [("medical",), ("unknown_value",)]
    df = spark.createDataFrame(data, ["loan_purpose"])

    result = standardize_loan_purpose(df).collect()

    assert result[0]["loan_purpose"] == "medical"
    assert result[1]["loan_purpose"] == "other"
```

## 9. Logging And Monitoring

Log:

- job name
- run date
- input path
- output path
- input count
- output count
- rejected count
- null counts
- duplicate counts
- runtime
- error details

Example:

```text
customers_cleaning started
input_count=2260701
duplicate_count=3317
null_annual_income_count=4
output_count=2257380
status=success
```

## 10. Data Quality Checks

Suggested checks:

```text
member_id should not be null
loan_id should not be null
annual_income should not be null
loan_amount should be greater than zero
funded_amount should be greater than zero
state should be 2 characters or NA
loan_purpose should be in allowed list
total_payment_received should not be zero after correction
```

## 11. Resource Estimation Answer

Sample:

```text
We estimated resources based on input size, number of partitions, transformations, and shuffle operations. Since cleaning was mostly narrow transformations with some aggregations for quality checks, the job was not as shuffle-heavy as large joins. We started with a baseline executor configuration, monitored Spark UI for task duration, spills, and executor memory, and adjusted executor memory and cores if needed. For production, we avoided very large executors and preferred balanced executor sizing.
```

## 12. Cluster Size Answer

Use realistic phrasing:

```text
In the lab environment, the cluster size was limited. In a production-like setup, we would run on a YARN or cloud-managed Spark cluster with multiple worker nodes. The exact executor configuration depends on daily data volume, SLA, and shuffle intensity. For this project, since the source was around a few GB and cleaning was mostly column transformations, moderate executor resources were enough.
```

Do not invent a very large cluster if you cannot defend it.

## 13. CI/CD Answer

```text
During development, we validated the logic in notebooks with sample data. For deployment, the code would be modularized into Python files, unit tested using pytest, configured through environment-specific config files, and deployed through a CI/CD pipeline. The same codebase would move from dev to stage and then prod with different input/output paths and resource configurations.
```

## 14. SCD Answer

For customer address changes:

Type 1:

```text
overwrite old address
```

Type 2:

```text
preserve old and new address with effective dates
```

Example:

```text
member_id | address_state | start_date | end_date | is_current
101       | KA            | 2023-01-01 | 2023-06-01 | false
101       | TS            | 2023-06-02 | null       | true
```

Use Type 2 when historical reporting or compliance matters.

## 15. Common Interview Questions And Answers

### Q1. What was the business problem?

The business wanted cleaned lending data for borrower risk analysis and loan approval decisioning.

### Q2. What were the main datasets?

Customers, loans, loan repayments, and loan defaulters.

### Q3. What cleaning did you do?

Schema enforcement, column renaming, duplicate removal, null handling, datatype conversion, categorical standardization, payment correction, and defaulter dataset splitting.

### Q4. Why did you use SHA-2?

To generate a deterministic surrogate member ID from stable borrower attributes when a reliable member ID was not available.

### Q5. Why Parquet?

Parquet is columnar, compressed, splittable, and efficient for Spark analytics. It supports predicate pushdown and column pruning.

### Q6. How did you handle nulls?

It depended on the column. Critical nulls such as annual income or loan amount were dropped. Employment length nulls were filled with average. Invalid date placeholders were converted to null.

### Q7. How did you test?

By writing unit tests for transformation functions using small Spark DataFrames and checking expected outputs.

### Q8. How did you handle bad records?

In a production design, bad records should be written to a quarantine path with rejection reason. In the learning implementation, critical nulls were dropped according to cleaning rules.

### Q9. How did you tune performance?

Used explicit schema, reduced columns, wrote Parquet outputs, checked Spark UI, avoided unnecessary shuffles, and avoided single-file writes for production scale.

### Q10. What would you improve?

I would modularize the code, add config-driven paths, add data quality framework checks, write rejected records separately, add logging/metrics, create CI/CD deployment, and add orchestration.

## 16. Production Checklist

Before production:

- schema defined explicitly
- paths parameterized
- no hardcoded usernames
- no notebook-only code
- transformations modularized
- unit tests written
- logging added
- rejected records stored
- data quality thresholds defined
- output format selected intentionally
- Spark configs documented
- job scheduled
- monitoring and alerting configured
- rollback or reprocessing plan available

## 17. Quick Revision

- Explain the project as business problem plus pipeline design.
- Be specific about datasets and cleaning rules.
- Mention Spark transformations by name.
- Explain dev/stage/prod and CI/CD.
- Discuss testing, logging, and data quality.
- Keep resource and cluster answers realistic.
