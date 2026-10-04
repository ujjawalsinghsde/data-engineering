# Spark Submit And Resource Configuration

## Introduction

Notebooks are good for development.

Production jobs should be packaged and submitted to the cluster.

Spark provides `spark-submit` for this.

Typical workflow:

```text
Develop logic in notebook
      |
      v
Move logic into .py file
      |
      v
Test with spark-submit in client mode
      |
      v
Run scheduled job in cluster mode
```

## Why Spark Submit?

Use `spark-submit` when:

- running production jobs
- scheduling jobs
- controlling resources
- running code outside notebook
- deploying reusable pipelines

Notebook:

- interactive
- good for exploration
- driver usually on gateway/client

Spark submit:

- repeatable
- configurable
- production friendly
- can run in cluster mode

## Basic PySpark Program

Example `prog1.py`:

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession
    .builder
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)

orders_schema = """
order_id long,
order_date string,
cust_id long,
order_status string
"""

orders_df = (
    spark.read
    .format("csv")
    .schema(orders_schema)
    .load("/public/trendytech/orders/orders_1gb.csv")
)

print("Number of partitions:", orders_df.rdd.getNumPartitions())

orders_df.createOrReplaceTempView("orders")

spark.sql("""
    SELECT order_status, COUNT(*) AS total
    FROM orders
    GROUP BY order_status
""").show()

spark.stop()
```

Important:

Do not hardcode lab usernames in scripts.

Use:

```python
import getpass
username = getpass.getuser()
```

## Client Mode Spark Submit

Client mode means driver runs on the gateway/client node.

Command:

```bash
spark-submit \
  --master yarn \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 2G \
  --conf spark.dynamicAllocation.enabled=false \
  prog1.py
```

Use client mode when:

- testing from terminal
- want print output in terminal
- debugging
- short-running jobs

Risk:

If gateway disconnects, driver can fail.

## Cluster Mode Spark Submit

Cluster mode means driver runs inside the cluster.

Command:

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 2G \
  --conf spark.dynamicAllocation.enabled=false \
  prog1.py
```

Use cluster mode when:

- production job
- scheduled job
- long-running job
- gateway should not hold driver

In cluster mode, print output may not appear directly in gateway terminal.

Check logs from YARN/ResourceManager.

## Driver Resources

Set driver memory and cores:

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --num-executors 3 \
  --executor-cores 4 \
  --executor-memory 2G \
  --driver-memory 2G \
  --driver-cores 2 \
  --conf spark.dynamicAllocation.enabled=false \
  --verbose \
  prog1.py
```

Driver needs enough memory when:

- collecting metadata
- building large query plans
- handling many tasks
- broadcasting variables
- running driver-side logic

Avoid:

```python
df.collect()
```

on large data because it brings data to driver.

## Static Resource Allocation

Static allocation means I specify fixed executors.

Key options:

```bash
--num-executors 3
--executor-cores 4
--executor-memory 2G
--conf spark.dynamicAllocation.enabled=false
```

Total executor cores:

```text
num executors * executor cores
```

Example:

```text
3 executors * 4 cores = 12 cores
```

So maximum parallel tasks:

```text
12
```

## Dynamic Allocation

Dynamic allocation lets Spark add/remove executors based on workload.

In many lab clusters, it is enabled by default.

Turn it off:

```bash
--conf spark.dynamicAllocation.enabled=false
```

Use static allocation when:

- benchmarking
- assignment requires fixed executors
- predictable resources needed
- debugging partition/task behavior

Downside:

Resources may sit idle if there are not enough tasks.

## Resource Configuration Meaning

```bash
--num-executors 3
```

Request 3 executor containers.

```bash
--executor-cores 4
```

Each executor has 4 CPU cores.

```bash
--executor-memory 2G
```

Each executor has 2 GB memory.

```bash
--driver-memory 2G
```

Driver gets 2 GB memory.

```bash
--driver-cores 2
```

Driver gets 2 cores.

```bash
--verbose
```

Prints effective configurations and submission details.

## Configuration Precedence

If the same config is set in multiple places, priority matters.

Priority:

1. SparkSession code configs
2. `spark-submit` configs
3. default config files like `spark-defaults.conf`

Example default config path:

```text
/etc/spark3/conf/spark-defaults.conf
```

Be careful:

If a config is hardcoded inside code, it can override submit-time configs.

For production, avoid hardcoding resource configs in the script unless intentional.

## `spark-submit` Vs `spark3-submit`

Some clusters have multiple Spark versions.

Commands may be:

```bash
spark-submit
```

or:

```bash
spark3-submit
```

Use the command matching the cluster environment.

## Client Mode Assignment Pattern

Requirement:

Run a Python file using:

- 2 executors
- 2 cores each
- 4 GB memory each
- client mode

Command:

```bash
spark-submit \
  --master yarn \
  --num-executors 2 \
  --executor-cores 2 \
  --executor-memory 4G \
  --conf spark.dynamicAllocation.enabled=false \
  question4.py
```

Why no `--deploy-mode cluster`?

Default is client mode in many Spark setups.

Client mode helps see print statements in terminal.

## Cluster Mode Assignment Pattern

Requirement:

- 4 executors
- 1 core each
- 2 GB executor memory
- 2 GB driver memory
- 1 driver core
- verbose
- cluster mode

Command:

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

## Writing Production Scripts

Good script should:

- create SparkSession
- avoid hardcoded user-specific paths
- define schema explicitly
- log important counts
- write output deterministically
- stop SparkSession

Example:

```python
import getpass
from pyspark.sql import SparkSession

def create_spark():
    username = getpass.getuser()
    return (
        SparkSession.builder
        .appName("users-analysis")
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
        .enableHiveSupport()
        .master("yarn")
        .getOrCreate()
    )

def main():
    spark = create_spark()
    try:
        # pipeline code here
        pass
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

## Common Mistakes

- Hardcoding lab usernames in code.
- Running production jobs in client mode.
- Forgetting `spark.stop()`.
- Setting configs in code and wondering why submit configs do not work.
- Asking for more executor memory than YARN maximum allows.
- Using too many executor cores and hurting throughput.
- Forgetting to disable dynamic allocation during fixed-resource experiments.
- Expecting cluster mode print output in local terminal.

## Best Practices

- Use notebooks for development, `spark-submit` for production.
- Use cluster mode for scheduled jobs.
- Use client mode for debugging.
- Use `--verbose` when learning/debugging configs.
- Keep resource configs outside code when possible.
- Use `getpass.getuser()` for user paths in labs.
- Check ResourceManager UI and Spark UI after submission.

## Interview Questions

### Beginner Questions

- What is `spark-submit`?
- What is client mode?
- What is cluster mode?
- What is executor memory?
- What is executor core?

### Intermediate Questions

- How do you disable dynamic allocation?
- How do you request 3 executors with 4 cores each?
- Where does the driver run in client mode?
- Where does the driver run in cluster mode?
- What is configuration precedence in Spark?

### Senior Data Engineer Questions

- How would you package and deploy a PySpark job for production?
- How do you choose executor cores and memory?
- Why should resource configs usually not be hardcoded in code?
- How would you debug a failed cluster-mode job?
- How do Airflow and spark-submit work together?

## Scenario-Based Questions

### Scenario 1: Submit Config Not Taking Effect

Possible reason:

Config is set inside SparkSession code and overrides submit config.

Fix:

- remove hardcoded config
- check `--verbose`
- check Spark UI Environment tab

### Scenario 2: Job Dies When Terminal Closes

Likely running in client mode.

Fix:

Use cluster deploy mode for production.

### Scenario 3: Print Output Missing

In cluster mode, output goes to cluster logs, not local terminal.

Check YARN application logs.

## Quick Revision

- Notebook = development.
- `spark-submit` = deployment.
- Client mode driver runs on gateway.
- Cluster mode driver runs in cluster.
- Static resources use `--num-executors`, `--executor-cores`, `--executor-memory`.
- Disable dynamic allocation for fixed-resource experiments.
- Config precedence: code > submit > defaults.
