# Logging With Log4j In PySpark

## 1. Why Not Use Print Statements?

`print()` is fine for quick exploration, but not for production.

Problems with print statements:

1. No logging levels like `INFO`, `WARN`, `ERROR`, `FATAL`.
2. Hard to disable or filter thousands of print statements.
3. Can slow down applications.
4. No consistent message format.
5. Harder to route logs to files or monitoring systems.

Production applications should use a logging framework.

## 2. Why Log4j?

Spark internally uses Log4j for JVM-side logging.

In PySpark, we can access Log4j through the Spark JVM gateway:

```python
spark._jvm.org.apache.log4j
```

This lets application logs follow the same logging infrastructure as Spark.

## 3. Logging Levels

Priority order:

```text
DEBUG < INFO < WARN < ERROR < FATAL
```

If logging level is set to `WARN`, visible logs are:

```text
WARN
ERROR
FATAL
```

Hidden logs:

```text
DEBUG
INFO
```

## 4. Log Targets

Logs can be written to:

- console
- file
- external logging systems

For learning, console logging is enough.

In production, logs usually go to:

- cluster logs
- driver logs
- executor logs
- centralized monitoring/logging tools

## 5. `log4j.properties`

Example:

```properties
log4j.rootCategory=INFO, console

log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
```

Set root logging level:

```text
INFO
WARN
ERROR
```

## 6. Configure Spark Session To Use Log4j Properties

`lib/Utils.py`

```python
from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties"
            ) \
            .master("local[2]") \
            .getOrCreate()

    return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()
```

For cluster mode, the `log4j.properties` file must be available to the driver. In real deployments, ship it using `--files` or package it appropriately.

Example:

```bash
spark-submit \
  --files log4j.properties \
  application_main.py PROD
```

## 7. Logger Wrapper

`lib/logger.py`

```python
class Log4j:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("retail_analysis")

    def error(self, message):
        self.logger.error(message)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)
```

Why create a wrapper?

- simple API for Python code
- consistent logger name
- avoids repeating JVM access code

## 8. Use Logger In Main Application

`application_main.py`

```python
import sys
from lib import DataManipulation, DataReader, Utils
from lib.logger import Log4j


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.warn("Created Spark Session")

    orders_df = DataReader.read_orders(spark, job_run_env)
    logger.info("Orders data loaded")

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    logger.info("Closed orders filtered")

    customers_df = DataReader.read_customers(spark, job_run_env)
    logger.info("Customers data loaded")

    joined_df = DataManipulation.join_orders_customers(
        orders_filtered,
        customers_df
    )
    logger.info("Orders and customers joined")

    aggregated_results = DataManipulation.count_orders_state(joined_df)
    aggregated_results.show(50)

    logger.info("Application completed successfully")
```

## 9. What To Log In Spark Jobs

Log:

- job start and end
- environment
- input paths
- output paths
- important configs
- input counts
- output counts
- rejected counts
- data quality failures
- major transformation steps
- exceptions

Example:

```python
logger.info(f"Running environment: {job_run_env}")
logger.info("Reading orders data")
logger.info(f"Closed orders count: {orders_filtered.count()}")
```

Be careful:

Calling `count()` just for logging triggers Spark jobs. Do this only when counts are required.

## 10. Logging Exceptions

Example:

```python
try:
    orders_df = DataReader.read_orders(spark, job_run_env)
except Exception as err:
    logger.error(f"Failed to read orders data: {err}")
    raise
```

Always re-raise exceptions unless you have a clear recovery plan.

## 11. Log Levels In Practice

Use `INFO` for:

- job started
- data loaded
- transformation completed
- job completed

Use `WARN` for:

- data quality threshold near limit
- optional input missing
- fallback logic used

Use `ERROR` for:

- read/write failure
- schema mismatch
- unrecoverable data issue

Use `DEBUG` for:

- detailed troubleshooting
- development-only details

## 12. Common Mistakes

1. Logging too much inside row-level transformations.
2. Triggering expensive actions only for logs.
3. Using `print()` in production code.
4. Not shipping `log4j.properties` with cluster jobs.
5. Logging sensitive data like passwords or customer PII.
6. Swallowing exceptions after logging them.

## 13. Production Logging Guidelines

Good production log line:

```text
INFO lending_pipeline: customers_cleaning completed input_count=2260701 output_count=2257380 rejected_count=3321
```

Avoid:

```text
INFO done
```

Include enough context to debug failures without opening the code.

## 14. Interview Questions

### Beginner

1. Why is logging better than print?
2. What are common logging levels?
3. What is Log4j?
4. Why does Spark use Log4j?
5. What is a log appender?

### Intermediate

1. How do you configure Log4j in PySpark?
2. How do you access Log4j from SparkSession?
3. What should be logged in a data pipeline?
4. Why should we avoid `count()` only for logging?
5. How do you log exceptions?

### Senior

1. How would you design logging for hundreds of Spark jobs?
2. How do logs support production incident debugging?
3. What should never be logged?
4. How do you connect Spark logs to centralized observability?
5. How do you balance useful logs and log noise?

## 15. Quick Revision

- Avoid print statements in production.
- Spark uses Log4j internally.
- PySpark can access Log4j through `spark._jvm`.
- Configure `log4j.properties`.
- Use meaningful levels: `INFO`, `WARN`, `ERROR`.
- Do not log sensitive data.
