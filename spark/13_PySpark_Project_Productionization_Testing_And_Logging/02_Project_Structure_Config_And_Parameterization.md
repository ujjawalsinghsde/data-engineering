# PySpark Project Structure, Config, And Parameterization

## 1. Why Move From Notebooks To A Project?

Notebooks are excellent for:

- exploration
- quick experiments
- understanding data
- developing first version of logic

Production jobs should be written as modular code because they need:

- testing
- version control
- code review
- CI/CD
- logging
- config management
- reusable functions
- environment-specific behavior

## 2. Example Retail Project Problem

Problem statement:

```text
Find the number of CLOSED orders for each state.
```

Inputs:

```text
customers.csv
orders.csv
```

Output:

```text
state, count
```

Flow:

```text
Read configs
Create Spark session
Read orders
Filter CLOSED orders
Read customers
Join on customer_id
Group by state
Show or write result
```

## 3. Recommended Project Layout

```text
retail_analysis/
  configs/
    application.conf
    pyspark.conf
  data/
    customers.csv
    orders.csv
    test_result/
      state_aggregate.csv
  lib/
    ConfigReader.py
    DataReader.py
    DataManipulation.py
    Utils.py
    logger.py
  tests/
    conftest.py
    test_retail_project.py
  application_main.py
  pytest.ini
  Pipfile
  Pipfile.lock
```

## 4. Application Config

`configs/application.conf`

```ini
[LOCAL]
customers.file.path = data/customers.csv
orders.file.path = data/orders.csv

[TEST]
customers.file.path = data/customers.csv
orders.file.path = data/orders.csv

[PROD]
customers.file.path = /data/retail/customers
orders.file.path = /data/retail/orders
```

Purpose:

- keep file paths outside code
- support multiple environments
- avoid hardcoding

## 5. PySpark Config

`configs/pyspark.conf`

```ini
[LOCAL]
spark.app.name = retail-local

[TEST]
spark.app.name = retail-test
spark.executor.instances = 3
spark.executor.cores = 5
spark.executor.memory = 15G

[PROD]
spark.app.name = retail-prod
spark.executor.instances = 3
spark.executor.cores = 5
spark.executor.memory = 15G
```

Purpose:

- keep Spark resource configs environment-specific
- run small locally
- run with cluster resources in test/prod

## 6. ConfigReader

`lib/ConfigReader.py`

```python
import configparser
from pyspark import SparkConf


def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.conf")

    app_conf = {}
    for key, val in config.items(env):
        app_conf[key] = val

    return app_conf


def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")

    pyspark_conf = SparkConf()
    for key, val in config.items(env):
        pyspark_conf.set(key, val)

    return pyspark_conf
```

Common naming mistake:

Make sure the function name used in `Utils.py` matches `get_pyspark_config`, not a different name like `get_spark_conf`.

## 7. DataReader

`lib/DataReader.py`

```python
from lib import ConfigReader


def get_customers_schema():
    return """
    customer_id int,
    customer_fname string,
    customer_lname string,
    username string,
    password string,
    address string,
    city string,
    state string,
    pincode string
    """


def get_orders_schema():
    return """
    order_id int,
    order_date string,
    customer_id int,
    order_status string
    """


def read_customers(spark, env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)


def read_orders(spark, env):
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]

    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)
```

## 8. DataManipulation

`lib/DataManipulation.py`

```python
def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")


def filter_orders_generic(orders_df, status):
    return orders_df.filter("order_status = '{}'".format(status))


def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")


def count_orders_state(joined_df):
    return joined_df.groupBy("state").count()
```

These functions are intentionally small. Small functions are easier to unit test.

## 9. Utils

`lib/Utils.py`

```python
from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .master("local[2]") \
            .getOrCreate()

    return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()
```

Local mode:

```text
master("local[2]")
```

Cluster mode:

```text
Spark configs come from spark-submit or config files.
```

## 10. Main Application

`application_main.py`

```python
import sys
from lib import DataManipulation, DataReader, Utils


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Created Spark Session")

    orders_df = DataReader.read_orders(spark, job_run_env)
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark, job_run_env)

    joined_df = DataManipulation.join_orders_customers(
        orders_filtered,
        customers_df
    )

    aggregated_results = DataManipulation.count_orders_state(joined_df)
    aggregated_results.show()

    print("End of main")
```

Run:

```bash
python application_main.py LOCAL
```

With Spark submit:

```bash
spark-submit application_main.py PROD
```

## 11. Why Parameterize Environment?

Same code should run in:

```text
LOCAL
TEST
PROD
```

Only configs should change.

Benefits:

- safer deployment
- less code duplication
- easy testing
- environment-specific paths/resources

## 12. Common Mistakes

1. Hardcoding file paths in code.
2. Hardcoding Spark configs inside transformation logic.
3. Putting all logic in `application_main.py`.
4. Reading configs inside every transformation function.
5. Using notebooks as production jobs without modularizing.
6. Not passing environment as a runtime argument.

## 13. Production Improvements

Add:

- argument parser instead of raw `sys.argv`
- logging framework
- exception handling
- output writer module
- data quality module
- unit tests
- integration tests
- config validation

## 14. Interview Questions

### Beginner

1. Why should PySpark projects be modular?
2. What is the purpose of config files?
3. What is `SparkConf`?
4. Why do we pass environment as argument?
5. What is the role of `application_main.py`?

### Intermediate

1. How do you separate local and production configs?
2. Why should transformation functions be small?
3. How do you avoid hardcoded paths?
4. How do you structure a PySpark project?
5. How would you add a writer module?

### Senior

1. How would you design config management for many pipelines?
2. How would you make the pipeline idempotent?
3. How do you validate configs before running?
4. How would you package this for CI/CD?
5. How would you support multiple regions or tenants?

## 15. Quick Revision

- Notebooks are for exploration; projects are for production.
- Keep configs in files.
- Keep read, transform, session, and main logic separate.
- Use environment parameterization.
- Small functions make unit testing easy.
