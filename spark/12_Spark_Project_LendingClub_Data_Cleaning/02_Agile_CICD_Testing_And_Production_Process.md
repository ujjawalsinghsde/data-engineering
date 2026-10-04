# Agile, CI/CD, Testing, And Production Process

## 1. Why Process Matters

In interviews, explaining PySpark code is only one part. Senior data engineering interviews also test whether you understand how real teams deliver production pipelines.

You should be able to explain:

- how work is assigned
- how requirements are broken down
- how code moves from dev to prod
- how testing is done
- how logging and monitoring are added
- how pipelines are parameterized
- how deployments are automated

## 2. Agile Vs Waterfall

Waterfall:

```text
Requirements -> Design -> Build -> Test -> Deploy
```

Usually long cycles. Feedback comes late.

Agile:

```text
Small increments delivered in sprints
```

Agile is preferred for data projects because:

- requirements evolve
- stakeholders need early demos
- data issues are discovered gradually
- pipelines can be delivered incrementally

## 3. Sprint-Based Development

A sprint is usually 1 or 2 weeks.

At the end of each sprint, the team should have something demonstrable.

Example sprint goals:

- create raw ingestion pipeline
- clean customer data
- standardize loan attributes
- write curated parquet outputs
- add data quality checks
- tune Spark job performance

## 4. Agile Hierarchy

Work is commonly broken down like this:

```text
Initiative
  -> Epic
    -> Story
      -> Task
        -> Sub-task
```

Example:

```text
Initiative:
Build lending risk analytics platform

Epic:
Create cleaned LendingClub data layer

Story:
Clean customers dataset

Tasks:
Enforce schema
Remove duplicates
Handle null income
Standardize employment length
Write parquet output
```

## 5. Story Points

Story points estimate effort and complexity.

Typical scale:

| Points | Meaning |
|---:|---|
| 1 | Few hours |
| 2 | About one day |
| 3 | Around two days |
| 5 | Three to four days |
| 8+ | Too large; should probably be split |

Example stories:

```text
Implement logging for the lending pipeline -> 2 points
Bring customers data into the data lake -> 3 points
Clean loan repayment data -> 5 points
Add data quality checks -> 3 points
```

## 6. Scrum Roles

Common roles:

1. Product Owner
2. Scrum Master
3. Developers
4. Stakeholders

### Product Owner

Owns the backlog and business priority.

Responsibilities:

- gather requirements
- define acceptance criteria
- prioritize stories
- clarify business rules

### Scrum Master

Facilitates Scrum process.

Responsibilities:

- run ceremonies
- remove blockers
- protect sprint scope
- keep team aligned

### Developers

Build and test the solution.

In data engineering, developers may:

- write PySpark code
- create ingestion jobs
- implement data quality checks
- tune Spark jobs
- write unit tests
- prepare deployments

### Stakeholders

Consumers or sponsors of the project.

Examples:

- risk analytics team
- reporting team
- business managers
- compliance users

## 7. Scrum Ceremonies

### 7.1 Sprint Planning

Participants:

- developers
- Scrum Master
- Product Owner

Purpose:

- discuss backlog stories
- estimate story points
- decide sprint commitment
- clarify acceptance criteria

Example:

```text
Story: Clean customers data
Acceptance criteria:
- schema enforced
- duplicate rows removed
- null annual income removed
- employment length standardized
- state values standardized
- output written in parquet
```

### 7.2 Daily Standup

Usually 10 to 15 minutes.

Questions:

- What did I work on yesterday?
- What will I work on today?
- Any blockers?

Example:

```text
Yesterday I completed schema enforcement for customers data.
Today I will implement employment length cleanup and null replacement.
No blockers.
```

### 7.3 Sprint Review

Purpose:

- demo completed work
- gather stakeholder feedback

Example demo:

- show cleaned customer output
- show before/after row counts
- show data quality checks
- show sample parquet output

### 7.4 Sprint Retrospective

Purpose:

- discuss what went well
- discuss what can improve
- identify action items

Example:

```text
What went well:
Cleaning logic completed on time.

What can improve:
We need sample data earlier for testing edge cases.
```

## 8. Dev, Stage, Prod

Data pipelines are usually deployed across environments.

### Dev

Used by developers.

Characteristics:

- small datasets
- frequent changes
- notebooks or local branches
- lower resources

### Stage

Production-like validation.

Characteristics:

- controlled deployments
- larger datasets
- integration testing
- user acceptance testing

### Prod

Business-critical environment.

Characteristics:

- scheduled jobs
- monitoring
- alerting
- access controls
- rollback process

## 9. CI/CD For PySpark Pipelines

CI/CD means Continuous Integration and Continuous Deployment.

Typical flow:

```text
Developer branch
    |
Pull request
    |
Code review
    |
Unit tests
    |
Build package
    |
Deploy to dev
    |
Deploy to stage
    |
Approval
    |
Deploy to prod
```

For PySpark:

- package code as `.py` files or Python wheel
- keep configs separate
- run unit tests
- run lint checks
- deploy through scheduler or orchestration tool

## 10. Modular Architecture

Avoid writing everything in one notebook.

Better structure:

```text
project/
  configs/
    dev.conf
    stage.conf
    prod.conf
  src/
    main.py
    readers.py
    cleaners.py
    writers.py
    quality_checks.py
  tests/
    test_cleaners.py
```

Benefits:

- easier testing
- reusable functions
- cleaner deployment
- easier code review

## 11. Configuration And Parameterization

Do not hardcode:

- input paths
- output paths
- environment names
- file formats
- run dates
- database names

Example config:

```text
env=dev
input_path=/data/lending/raw
output_path=/data/lending/cleaned
output_format=parquet
run_date=2026-08-24
```

Pipeline should accept parameters:

```bash
spark-submit lending_pipeline.py \
  --env dev \
  --run-date 2026-08-24
```

## 12. Logging

Production jobs need logs.

Log:

- job start and end
- input path
- output path
- input count
- output count
- rejected count
- data quality failures
- runtime
- exception details

Example:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lending_pipeline")

logger.info("Starting customers cleaning job")
logger.info("Input path: %s", input_path)
logger.info("Output path: %s", output_path)
```

## 13. Unit Testing With Pytest

Unit tests validate transformation logic.

Example function:

```python
from pyspark.sql.functions import regexp_replace, col

def clean_emp_length(df):
    return df.withColumn(
        "emp_length",
        regexp_replace(col("emp_length"), r"(\\D)", "").cast("int")
    )
```

Example test:

```python
def test_clean_emp_length(spark):
    data = [("10+ years",), ("3 years",), ("< 1 year",)]
    df = spark.createDataFrame(data, ["emp_length"])

    result = clean_emp_length(df).collect()

    assert result[0]["emp_length"] == 10
    assert result[1]["emp_length"] == 3
```

In real tests, handle edge cases like `< 1 year` carefully based on business rule.

## 14. Data Quality Checks

Examples:

- primary key not null
- annual income not null
- loan amount greater than zero
- state code length equals 2
- purpose belongs to allowed list
- payment received not negative
- duplicate count within threshold

Example:

```python
bad_income_count = customers_df.filter("annual_income is null").count()

if bad_income_count > 0:
    logger.warning("Rows with null annual income: %s", bad_income_count)
```

## 15. Slowly Changing Dimensions

SCD means Slowly Changing Dimensions.

Example:

```text
Customer address changed:
Bangalore at time1
Hyderabad at time2
```

Common SCD types:

- Type 1: overwrite old value
- Type 2: preserve history with start date, end date, active flag

For risk or compliance systems, Type 2 is often important because history matters.

Example Type 2 columns:

```text
customer_id
address
effective_start_date
effective_end_date
is_current
```

## 16. Resource Estimation

Estimate resources using:

- daily data volume
- file format
- compression
- number of transformations
- shuffle-heavy operations
- joins
- SLA
- cluster limits

Example answer:

```text
For development we used smaller sample data and fewer executors. For production, we estimated resources based on input data size, number of partitions, shuffle volume, and SLA. We started with a baseline executor configuration, monitored Spark UI for spills and task skew, then tuned executor cores, memory, and shuffle partitions.
```

## 17. Common Interview Mistakes

1. Saying “I used Spark” without explaining why.
2. Not knowing data volume.
3. Not knowing cluster size.
4. Not explaining dev/stage/prod.
5. Not explaining unit testing.
6. Claiming production work was done entirely in notebooks.
7. Not having a clear business problem.

## 18. Quick Revision

- Agile delivers projects incrementally through sprints.
- Scrum roles include Product Owner, Scrum Master, developers, stakeholders.
- CI/CD moves code through dev, stage, prod.
- Production code should be modular, tested, logged, and parameterized.
- Use `pytest` for transformation unit tests.
- Explain SCD and resource estimation in interviews.
