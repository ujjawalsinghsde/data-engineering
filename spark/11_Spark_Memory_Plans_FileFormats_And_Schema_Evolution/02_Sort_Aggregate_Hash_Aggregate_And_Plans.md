# Sort Aggregate, Hash Aggregate, Logical Plans, And Physical Plans

## 1. Why Execution Plans Matter

Spark SQL and DataFrame code looks simple, but Spark performs many steps internally before running it.

Example:

```python
orders_df.groupBy("customer_id").count()
```

Spark has to decide:

- how to parse the query
- whether columns and tables exist
- whether filters can be pushed down
- whether projections can be simplified
- whether to use hash aggregation or sort aggregation
- whether to use broadcast join, sort merge join, or shuffle hash join

These decisions are visible through execution plans.

## 2. Sort Aggregate Vs Hash Aggregate

Both are physical strategies for aggregation.

Logical requirement:

```sql
SELECT customer_id, count(*)
FROM orders
GROUP BY customer_id
```

Physical implementation may be:

- Hash Aggregate
- Sort Aggregate

## 3. Hash Aggregate

Hash Aggregate uses a hash table.

Flow:

```text
Read row
  |
Calculate grouping key
  |
If key exists in hash table, update aggregate
  |
If key does not exist, create new hash entry
```

Example:

```text
Input:
(1, January)
(1, January)
(2, February)
(1, January)

Hash table:
1-January -> 3
2-February -> 1
```

Complexity:

```text
O(n)
```

Hash Aggregate is usually faster because lookup and update are efficient.

## 4. Sort Aggregate

Sort Aggregate sorts data by grouping keys first, then aggregates adjacent records.

Flow:

```text
Read rows
  |
Sort by grouping key
  |
Scan sorted records
  |
Aggregate same keys together
```

Complexity:

```text
O(n log n)
```

Sorting is expensive, especially for large datasets.

## 5. Why Spark May Choose Sort Aggregate

Spark may choose Sort Aggregate when:

- grouping keys or aggregate expressions are not friendly for hash aggregation
- memory pressure makes hash aggregation risky
- data types make hash map handling less efficient
- query shape includes ordering requirements
- Spark planner estimates sort-based execution as safer

In the course example, one query used string-based month values and Spark chose Sort Aggregate. Another query converted month number to integer and Spark chose Hash Aggregate, running much faster.

## 6. Example Queries

Load orders:

```python
orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/retail_db/ordersnew")

orders_df.createOrReplaceTempView("orders")
```

Query that may lead to Sort Aggregate:

```python
spark.sql("""
SELECT
    customer_id,
    date_format(order_date, 'MMMM') AS order_month,
    count(1) AS total_count
FROM orders
GROUP BY customer_id, order_month
ORDER BY order_month
""").explain(True)
```

Query that may lead to Hash Aggregate:

```python
spark.sql("""
SELECT
    customer_id,
    date_format(order_date, 'MMMM') AS order_month,
    count(1) AS total_count,
    first(int(date_format(order_date, 'MM'))) AS month_num
FROM orders
GROUP BY customer_id, order_month
ORDER BY month_num
""").explain(True)
```

Use:

```python
.write.format("noop").mode("overwrite").save()
```

when you want to execute for performance testing without writing actual output.

## 7. Parsed Logical Plan

The parsed logical plan checks syntax.

It answers:

```text
Is this valid SQL syntax?
```

It does not fully validate whether the table or column exists.

Example syntax problem:

```sql
SELECT FROM orders
```

This can cause a parse exception.

## 8. Analyzed Logical Plan

The analyzed logical plan resolves tables, views, columns, and functions using the catalog.

It answers:

```text
Does this table exist?
Does this column exist?
Are references valid?
```

Example:

```python
spark.sql("SELECT * FROM order")
```

If the table is actually named `orders`, Spark throws an analysis exception.

## 9. Optimized Logical Plan

The optimized logical plan applies rule-based optimizations.

Common optimizations:

- predicate pushdown
- projection pruning
- combining filters
- combining projections
- constant folding
- simplifying expressions

Example:

```python
spark.sql("""
SELECT order_id, order_status
FROM (
    SELECT order_id, customer_id, order_status
    FROM orders
    WHERE order_id < 500
)
WHERE order_id < 200
""").explain(True)
```

Spark can combine filters:

```text
order_id < 500 AND order_id < 200
```

into the stronger filter:

```text
order_id < 200
```

It can also remove unused columns like `customer_id`.

## 10. Physical Plan

The physical plan decides how Spark will actually run the query.

Examples of physical decisions:

- Broadcast Hash Join or Sort Merge Join
- Hash Aggregate or Sort Aggregate
- number of exchanges
- shuffle boundaries
- scan strategy
- pushed filters

Use:

```python
df.explain(True)
```

or:

```python
spark.sql("...").explain(True)
```

## 11. Plan Stages

The full `explain(True)` output commonly shows:

```text
Parsed Logical Plan
Analyzed Logical Plan
Optimized Logical Plan
Physical Plan
```

Mental model:

```text
SQL/DataFrame Code
        |
Parsed Logical Plan
        |
Analyzed Logical Plan
        |
Optimized Logical Plan
        |
Physical Plan
        |
Execution
```

## 12. Catalyst Optimizer

Catalyst is Spark SQL’s optimizer.

It applies rules to transform query plans into more efficient plans.

Catalyst helps with:

- resolving attributes
- simplifying expressions
- pushing filters
- pruning unused columns
- choosing physical strategies
- enabling DataFrame and SQL APIs to share the same optimizer

This is one reason DataFrames and Spark SQL are usually faster and easier to optimize than raw RDD code.

## 13. Predicate Pushdown

Predicate pushdown means Spark pushes filters as close to the data source as possible.

Example:

```python
orders_df.filter("order_id < 200").select("order_id", "order_status")
```

If the file format supports metadata-based skipping, Spark may avoid reading irrelevant row groups or files.

Predicate pushdown is especially effective with:

- Parquet
- ORC
- partitioned tables
- indexed external systems

## 14. Projection Pruning

Projection pruning means Spark reads only required columns.

Example:

```python
orders_df.select("order_id", "customer_id")
```

With columnar formats like Parquet and ORC, Spark can avoid reading other columns.

This reduces:

- I/O
- memory usage
- network transfer
- CPU cost

## 15. Combining Filters And Projections

Spark can combine multiple filters:

```python
df.filter("order_id < 500").filter("order_id < 200")
```

Optimized:

```text
order_id < 200
```

Spark can combine projections:

```python
df.select("order_id", "customer_id", "order_status").select("order_id", "order_status")
```

Optimized:

```text
select only order_id and order_status
```

## 16. Join Plan Example

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

customers_df.createOrReplaceTempView("customers")

spark.sql("""
SELECT *
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id
WHERE o.order_status = 'CLOSED'
""").explain(True)
```

Look for:

- pushed filters
- join strategy
- exchange/shuffle
- broadcast
- aggregate strategy

## 17. Common Mistakes

1. Looking only at code and not checking the physical plan.
2. Assuming DataFrame API and SQL API have different optimizers.
3. Ignoring projection pruning and reading all columns.
4. Filtering after expensive joins when filter could happen before.
5. Assuming Hash Aggregate will always be chosen.
6. Treating `explain()` as optional for performance debugging.

## 18. Production Guidance

For performance tuning:

- use `explain(True)`
- check Spark UI
- compare logical and physical plans
- use Parquet/ORC for pushdown
- avoid unnecessary string transformations in grouping keys
- filter and project before joins
- understand why Spark chose a strategy before forcing hints

## 19. Interview Questions

### Beginner

1. What is a logical plan?
2. What is a physical plan?
3. What is Catalyst Optimizer?
4. What is predicate pushdown?
5. What is projection pruning?

### Intermediate

1. Difference between parsed and analyzed logical plan?
2. Why is Hash Aggregate usually faster than Sort Aggregate?
3. When might Spark choose Sort Aggregate?
4. How do you inspect a query plan?
5. What does `explain(True)` show?

### Senior

1. A query reads too much data despite filtering. What plan details do you inspect?
2. How do Catalyst rules improve DataFrame performance?
3. When would you avoid forcing a physical strategy with hints?
4. How can expression choices affect aggregate strategy?
5. How do predicate pushdown and file format design work together?

## 20. Quick Revision

- Hash Aggregate uses hash tables and is usually `O(n)`.
- Sort Aggregate sorts first and is usually `O(n log n)`.
- Parsed plan checks syntax.
- Analyzed plan resolves tables and columns.
- Optimized plan applies rules.
- Physical plan chooses execution strategies.
- Catalyst powers Spark SQL/DataFrame optimization.
