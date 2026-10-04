# Assignment Guide: Joins, AQE, And Join Optimization

## 1. Goal

The goal of this assignment is to understand:

- when to use different join types
- when Spark chooses different join strategies
- how to inspect joins in Spark UI
- how AQE changes shuffle behavior
- how to demonstrate left outer and left semi joins

## 2. Datasets

You can use any one large dataset and one small dataset.

Example lab paths:

```text
Large dataset:
/public/trendytech/orders/orders_1gb.csv

Small dataset:
/public/trendytech/retail_db/customers
```

In real projects:

- large dataset could be transactions, orders, logs, clickstream, claims
- small dataset could be customers, products, countries, stores, plans

## 3. Spark Session

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
    .enableHiveSupport() \
    .master("yarn") \
    .getOrCreate()
```

## 4. Load DataFrames

```python
orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")
```

```python
customers_schema = """
customer_id long,
customer_fname string,
customer_lname string,
user_name string,
password string,
address string,
city string,
state string,
pincode long
"""

customers_df = spark.read \
    .format("csv") \
    .schema(customers_schema) \
    .load("/public/trendytech/retail_db/customers")
```

Check counts and partitions:

```python
print("Orders partitions:", orders_df.rdd.getNumPartitions())
print("Customers partitions:", customers_df.rdd.getNumPartitions())

print("Orders count:", orders_df.count())
print("Customers count:", customers_df.count())
```

## 5. DataFrame Join

Use the business key:

```python
joined_df = orders_df.join(customers_df, "customer_id", "inner")
```

Trigger execution without writing output:

```python
joined_df.write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Check:

```python
joined_df.explain(True)
```

Look for:

```text
BroadcastHashJoin
SortMergeJoin
ShuffledHashJoin
```

## 6. Spark SQL Join

```python
orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")

sql_joined_df = spark.sql("""
SELECT *
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id
""")

sql_joined_df.write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Important note:

DataFrame API and Spark SQL API both go through Spark’s optimizer. The style is different, but the execution engine is the same.

## 7. Disable Broadcast Join

Check broadcast threshold:

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

Disable broadcast:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Run join again:

```python
no_broadcast_df = orders_df.join(customers_df, "customer_id", "inner")

no_broadcast_df.write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Inspect:

```python
no_broadcast_df.explain(True)
```

Expected strategy is commonly:

```text
SortMergeJoin
```

## 8. Shuffle Hash Join Hint

```python
shuffle_hash_df = orders_df.join(
    customers_df.hint("shuffle_hash"),
    "customer_id",
    "inner"
)

shuffle_hash_df.write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Inspect:

```python
shuffle_hash_df.explain(True)
```

Spark may or may not follow a hint if it determines the hint is invalid or unsafe. A hint is guidance, not an absolute guarantee.

## 9. Enable AQE And Observe Shuffle Partitions

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Run:

```python
aqe_join_df = orders_df.join(customers_df, "customer_id", "inner")

aqe_join_df.write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Check Spark UI:

- initial shuffle partitions
- final coalesced partitions
- adaptive plan
- task count
- shuffle read size

Also inspect:

```python
aqe_join_df.explain(True)
```

## 10. Demonstrate Left Outer Join

Use small sample data to make the result easy to understand.

```python
employee_data = [
    (10, "Raj", "1999", "100", "M", 2000),
    (20, "Rahul", "2002", "200", "M", 2000),
    (30, "Raghav", "2010", "100", "", 2000),
    (40, "Reema", "2004", "100", "F", 2000),
    (50, "Rina", "2008", "400", "F", 2000),
    (60, "Rasul", "2014", "500", "M", 2000)
]

employee_schema = [
    "employee_id",
    "name",
    "doj",
    "employee_dept_id",
    "gender",
    "salary"
]

employee_df = spark.createDataFrame(employee_data, employee_schema)
```

```python
department_data = [
    ("HR", "100"),
    ("Supply", "200"),
    ("Sales", "300"),
    ("Stock", "400")
]

department_schema = ["dept_name", "dept_id"]

department_df = spark.createDataFrame(department_data, department_schema)
```

Left outer join:

```python
left_join_df = employee_df.join(
    department_df,
    employee_df.employee_dept_id == department_df.dept_id,
    "left_outer"
)

left_join_df.show()
```

Use case:

```text
Show all employees, even if their department mapping is missing.
```

Employees with department `500` will appear with `NULL` department columns.

## 11. Demonstrate Left Semi Join

```python
semi_join_df = employee_df.join(
    department_df,
    employee_df.employee_dept_id == department_df.dept_id,
    "left_semi"
)

semi_join_df.show()
```

Use case:

```text
Show employees who belong to a valid department.
```

Only employee columns are returned. Department columns are not included.

## 12. SQL Version

```python
employee_df.createOrReplaceTempView("employees")
department_df.createOrReplaceTempView("departments")
```

Left outer:

```python
spark.sql("""
SELECT *
FROM employees e
LEFT OUTER JOIN departments d
ON e.employee_dept_id = d.dept_id
""").show()
```

Left semi:

```python
spark.sql("""
SELECT *
FROM employees e
LEFT SEMI JOIN departments d
ON e.employee_dept_id = d.dept_id
""").show()
```

## 13. Optional: Left Anti Join

```python
anti_join_df = employee_df.join(
    department_df,
    employee_df.employee_dept_id == department_df.dept_id,
    "left_anti"
)

anti_join_df.show()
```

Use case:

```text
Find employees with invalid or missing department mappings.
```

## 14. What To Capture From Spark UI

For each join experiment, capture:

- physical join strategy
- number of stages
- number of tasks
- shuffle read
- shuffle write
- broadcast size if broadcast join is used
- task duration distribution
- whether AQE modified the plan

Suggested observation table:

| Scenario | Broadcast Enabled | AQE Enabled | Hint | Join Strategy | Shuffle Partitions | Notes |
|---|---:|---:|---|---|---:|---|
| Default join | Yes | Environment default | None | BroadcastHashJoin likely | Low | Small table broadcast |
| Broadcast disabled | No | Environment default | None | SortMergeJoin likely | 200 before AQE | Shuffle required |
| Shuffle hash hint | No | Environment default | shuffle_hash | ShuffledHashJoin possible | 200 before AQE | Depends on Spark decision |
| AQE enabled | No | Yes | None | Adaptive plan | Coalesced possible | Check final plan |

## 15. Practical Notes

1. `explain(True)` is useful, but Spark UI gives better runtime evidence.
2. Broadcast join is usually fastest when one side is truly small.
3. Sort merge join is common and reliable for large joins.
4. Shuffle hash join can help, but can also increase memory pressure.
5. AQE can improve runtime plans, but it cannot fix every bad data model.
6. Join keys must have matching data types.

## 16. Clean Up

If you created temporary tables:

```python
spark.catalog.dropTempView("orders")
spark.catalog.dropTempView("customers")
spark.catalog.dropTempView("employees")
spark.catalog.dropTempView("departments")
```

If you changed configs for testing, reset them or restart the notebook kernel.

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

## 17. Interview-Ready Explanation

When Spark joins two DataFrames, it first decides the logical result based on join type, such as inner, left outer, semi, or anti. Then the optimizer chooses a physical join strategy. If one side is small, Spark may use Broadcast Hash Join and avoid shuffling the large table. If both sides are large, Spark commonly uses Sort Merge Join, where both sides are shuffled by join key, sorted, and merged. With AQE enabled, Spark can adjust the plan at runtime by coalescing shuffle partitions, handling skew, or switching join strategies after it sees actual data sizes.

## 18. Quick Revision

- DataFrame and SQL joins use the same optimizer.
- Disable broadcast threshold to observe shuffle joins.
- Use join hints for experiments, but verify with Spark UI.
- AQE can change the physical plan at runtime.
- Left outer keeps all left rows.
- Left semi keeps only matching left rows and no right columns.
- Left anti keeps non-matching left rows.
