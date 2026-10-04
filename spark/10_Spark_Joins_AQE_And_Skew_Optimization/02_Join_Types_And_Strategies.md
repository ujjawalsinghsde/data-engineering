# Spark Join Types And Join Strategies

## 1. Why Joins Matter In Spark

Joins are one of the most expensive operations in Spark because they often require data movement across the network.

In a distributed system, rows with the same join key may be stored on different executors.

For a join to happen correctly, Spark must ensure matching keys come together.

Example:

```text
orders.customer_id = customers.customer_id
```

If order records for customer `101` are on executor 1, and the customer record for `101` is on executor 3, Spark has to move data unless it can use a broadcast join or bucketed layout.

## 2. Sample Data

Orders:

```text
order_id,customer_id,order_status
101,1,CLOSED
102,2,COMPLETE
103,3,PENDING
104,4,CLOSED
```

Customers:

```text
customer_id,city
1,bangalore
2,pune
5,mumbai
```

## 3. Common Join Types

### 3.1 Inner Join

Returns only matching records from both sides.

```text
Result keys: 1, 2
```

Use cases:

- orders with valid customer details
- transactions with matching account records
- order items with matching products

PySpark:

```python
orders_df.join(customers_df, "customer_id", "inner")
```

Spark SQL:

```sql
SELECT *
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id
```

### 3.2 Left Outer Join

Returns all records from the left table and matching records from the right table.

If no match exists, right-side columns become `NULL`.

```text
Matched: 1, 2
Left only: 3, 4
```

Use cases:

- all orders, even if customer record is missing
- all users, with optional profile details
- all transactions, with optional fraud score

PySpark:

```python
orders_df.join(customers_df, "customer_id", "left")
```

SQL:

```sql
SELECT *
FROM orders o
LEFT OUTER JOIN customers c
ON o.customer_id = c.customer_id
```

### 3.3 Right Outer Join

Returns all records from the right table and matching records from the left table.

If no match exists, left-side columns become `NULL`.

```text
Matched: 1, 2
Right only: 5
```

Use cases:

- all customers, with any orders if present
- all products, with any sales if present

In practice, many teams prefer rewriting right joins as left joins by swapping table order because it is easier to reason about.

### 3.4 Full Outer Join

Returns:

- matching records from both sides
- non-matching records from the left side
- non-matching records from the right side

Use cases:

- reconciliation between two systems
- comparing source and target datasets
- finding inserted, deleted, and matched records

PySpark:

```python
orders_df.join(customers_df, "customer_id", "full")
```

### 3.5 Left Semi Join

Returns only rows from the left table that have a match in the right table.

It does not return columns from the right table.

Use case:

```text
Find customers who placed at least one order.
```

```python
customers_df.join(orders_df, "customer_id", "left_semi")
```

Equivalent SQL idea:

```sql
SELECT *
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
)
```

Why it is useful:

- efficient filtering
- avoids duplicate right-side columns
- avoids unnecessary data expansion

### 3.6 Left Anti Join

Returns only rows from the left table that do not have a match in the right table.

Use case:

```text
Find customers who never placed an order.
```

```python
customers_df.join(orders_df, "customer_id", "left_anti")
```

Equivalent SQL idea:

```sql
SELECT *
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
)
```

## 4. Join Type Summary

| Join Type | Output |
|---|---|
| Inner | Matching rows only |
| Left outer | Matching rows plus non-matching left rows |
| Right outer | Matching rows plus non-matching right rows |
| Full outer | Matching rows plus non-matching rows from both sides |
| Left semi | Left rows that have a match |
| Left anti | Left rows that do not have a match |

## 5. Join Strategies

Join type describes the result.

Join strategy describes how Spark physically executes the join.

Common Spark join strategies:

1. Broadcast Hash Join
2. Shuffle Sort Merge Join
3. Shuffle Hash Join
4. Sort Merge Bucket Join

## 6. Broadcast Hash Join

Broadcast Hash Join is used when one table is small enough to fit in memory.

Default threshold is commonly:

```text
10 MB
```

Check:

```python
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
```

If a table is smaller than the threshold, Spark can broadcast it to all executors.

Example:

```text
orders    -> large table, distributed across executors
customers -> small table, copied to each executor
```

Flow:

```text
Small table collected to driver
        |
Driver broadcasts hash table to executors
        |
Each executor joins locally with its partition of large table
        |
No large shuffle required
```

Code:

```python
orders_df.join(customers_df, "customer_id", "inner")
```

Explicit hint:

```python
from pyspark.sql.functions import broadcast

orders_df.join(broadcast(customers_df), "customer_id", "inner")
```

Use broadcast join when:

- one side is small
- the small side fits safely in driver and executor memory
- the join is repeated or performance-critical

Avoid broadcast join when:

- the “small” table is not actually small after expansion
- driver memory is limited
- executor memory is tight
- the table is close to threshold but has many wide columns

## 7. Shuffle Sort Merge Join

Shuffle Sort Merge Join is a common strategy for joining two large datasets.

Steps:

1. Shuffle both datasets by join key.
2. Sort records by join key inside each shuffle partition.
3. Merge sorted records with matching keys.

Flow:

```text
orders partitions      customers partitions
        |                      |
        +---- shuffle by key --+
                  |
          same keys together
                  |
                sort
                  |
                merge
```

This is reliable for large joins but expensive because it involves shuffle and sort.

Use when:

- both tables are large
- broadcast is not possible
- join keys are sortable
- data is not already bucketed appropriately

## 8. Shuffle Hash Join

Shuffle Hash Join also shuffles data by key, but instead of sorting both sides, Spark builds a hash table on the smaller side within each partition.

Flow:

```text
Shuffle both sides by join key
        |
Build hash table from smaller side per partition
        |
Probe with larger side
```

Hint:

```python
orders_df.join(
    customers_df.hint("shuffle_hash"),
    orders_df.customer_id == customers_df.customer_id,
    "inner"
)
```

Use carefully. It can perform well when:

- both tables are not small enough for broadcast
- one side is smaller after shuffle
- hash table can fit in executor memory

It can be risky when:

- the build side is too large
- keys are skewed
- executor memory is limited

## 9. Disabling Broadcast For Testing

To observe a normal shuffle join:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Then run:

```python
orders_df.join(customers_df, "customer_id", "inner") \
    .write \
    .format("noop") \
    .mode("overwrite") \
    .save()
```

Check Spark UI physical plan to see whether Spark used:

- SortMergeJoin
- ShuffledHashJoin
- BroadcastHashJoin

## 10. Example With Schemas

```python
orders_schema = "order_id long, order_date string, customer_id long, order_status string"

orders_df = spark.read \
    .format("csv") \
    .schema(orders_schema) \
    .load("/public/trendytech/orders/orders_1gb.csv")

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

DataFrame join:

```python
joined_df = orders_df.join(customers_df, "customer_id", "inner")
```

SQL join:

```python
orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")

spark.sql("""
SELECT *
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id
""")
```

## 11. Common Mistakes

1. Joining on the wrong column, such as `order_id = customer_id`.
2. Using `inner` join when the business needs unmatched records too.
3. Using full outer joins casually on large tables.
4. Broadcasting a table that is too large.
5. Forgetting that duplicate keys can multiply output rows.
6. Not checking the physical plan.
7. Assuming SQL style and DataFrame style use different execution engines. They do not; both go through Spark’s optimizer.

## 12. Production Guidance

Before joining large datasets, ask:

- What is the join type required by business logic?
- Which side is smaller?
- Can the smaller side be broadcast?
- Are join keys skewed?
- Are both datasets filtered before join?
- Are only required columns selected?
- Is this join repeated enough to justify bucketing?
- Is AQE enabled?

Good pattern:

```python
filtered_orders = orders_df \
    .filter("order_date >= '2023-01-01'") \
    .select("order_id", "customer_id", "order_status")

selected_customers = customers_df \
    .select("customer_id", "city", "state")

result = filtered_orders.join(selected_customers, "customer_id", "inner")
```

Filter early. Select fewer columns. Join only what you need.

## 13. Interview Questions

### Beginner

1. What is the difference between inner join and left join?
2. What is a left semi join?
3. What is a left anti join?
4. What is broadcast join?
5. Why are joins expensive in Spark?

### Intermediate

1. How does Broadcast Hash Join work?
2. How does Sort Merge Join work?
3. When does Spark choose broadcast join?
4. How do you disable broadcast join?
5. Why can full outer joins be expensive?

### Senior

1. A join between a 1 TB table and a 5 MB table is slow. How would you optimize it?
2. A join output is much larger than expected. What would you investigate?
3. How would you choose between broadcast join, sort merge join, and shuffle hash join?
4. When can a broadcast join cause production instability?
5. How do join type and join strategy differ?

## 14. Quick Revision

- Join type defines result semantics.
- Join strategy defines physical execution.
- Broadcast Hash Join avoids large shuffle.
- Sort Merge Join is common for large joins.
- Shuffle Hash Join can help when one shuffled side is smaller.
- Left semi filters left rows with matches.
- Left anti filters left rows without matches.
- Always check Spark UI or `explain()`.
