# Joins, Broadcast Variables, And Practical RDD Patterns

## Introduction

Joins are common in data engineering.

Example:

- Orders has `customer_id`.
- Customers has `customer_id`.
- We join them to get customer information for each order.

In Spark RDDs, normal joins are wide transformations.

Wide transformation means shuffle happens.

This note covers:

- Standard RDD join.
- Why joins are expensive.
- Broadcast join.
- Practical order/customer examples.
- Common RDD patterns.

## Join Use Case

Orders dataset:

```text
order_id, order_date, customer_id, order_status
```

Example:

```text
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
```

Customers dataset:

```text
customer_id, fname, lname, username, password, address, city, state, pincode
```

Example:

```text
1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521
```

Common column:

```text
customer_id
```

## Standard RDD Join

RDD joins work on pair RDDs.

Both RDDs must be in this format:

```text
(join_key, value)
```

### Prepare Orders RDD

```python
orders_base = spark.sparkContext.textFile("/public/ujjawalsingh/orders/orders_1gb.csv")
orders_mapped = orders_base.map(lambda x: (x.split(",")[2], x.split(",")[3]))
```

Output:

```text
(customer_id, order_status)
```

Example:

```text
('11599', 'CLOSED')
```

### Prepare Customers RDD

```python
customers_base = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/customers/part-00000")
customers_mapped = customers_base.map(lambda x: (x.split(",")[0], x.split(",")[8]))
```

Output:

```text
(customer_id, pincode)
```

Example:

```text
('11599', '10001')
```

### Join

```python
joined_rdd = customers_mapped.join(orders_mapped)
```

Output:

```text
(customer_id, (pincode, order_status))
```

Example:

```text
('11599', ('10001', 'CLOSED'))
```

Save:

```python
joined_rdd.saveAsTextFile("data/orders_joined")
```

## Why Standard Join Is Expensive

For join to happen, same keys must come to the same partition.

Spark may need to shuffle both datasets.

Example:

```text
Orders:    1 GB, 9 partitions
Customers: 1 MB, 2 partitions
```

Normal join may move data across network so matching customer IDs are together.

Problems:

- Join is a wide transformation.
- It creates additional stages.
- Network shuffle can be large.
- Data skew can overload some partitions.
- Slow if both datasets are large.

## Broadcast Join

Broadcast join is an optimization when one dataset is small.

Idea:

```text
Large dataset remains distributed.
Small dataset is copied to every executor.
Each executor joins locally.
```

Diagram:

```text
Orders partition on Node 1 + full Customers lookup
Orders partition on Node 2 + full Customers lookup
Orders partition on Node 3 + full Customers lookup
```

No big shuffle is required.

Use broadcast join when:

- One dataset is small enough to fit in executor memory.
- Large dataset is much bigger.
- Join key lookup can be done locally.

Do not use broadcast join when:

- "Small" dataset is not actually small.
- Broadcast object is too large for executor memory.
- Small dataset changes too frequently and causes repeated broadcast cost.

## Broadcast Join Code

Load large orders:

```python
orders_base = spark.sparkContext.textFile("/public/ujjawalsingh/orders/orders_1gb.csv")
orders_mapped = orders_base.map(lambda x: (x.split(",")[2], x.split(",")[3]))
```

Load small customers:

```python
customers_base = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/customers")
customers_mapped = customers_base.map(lambda x: (x.split(",")[0], x.split(",")[8]))
```

Collect and broadcast:

```python
customers_broadcast = spark.sparkContext.broadcast(customers_mapped.collect())
```

Course lookup function:

```python
def getPinCode(customerID):
    try:
        for customer in customers_broadcast.value:
            if customer[0] == str(customerID):
                return (customer[0], customer[1])
        return -1
    except:
        return -1
```

Use broadcast:

```python
joined_rdd = orders_mapped.map(lambda x: (getPinCode(int(x[0])), x[1]))
joined_rdd.saveAsTextFile("data/broadcastresults")
```

## Better Broadcast Lookup Using Dictionary

The course code works, but lookup scans the customer list for every order.

Better approach:

```python
customers_dict = customers_mapped.collectAsMap()
customers_broadcast = spark.sparkContext.broadcast(customers_dict)

joined_rdd = orders_mapped.map(
    lambda x: (x[0], (x[1], customers_broadcast.value.get(x[0], None)))
)
```

Why better:

- Dictionary lookup is faster.
- `get(customer_id)` is direct.
- Avoids looping through all customers for every order.

Output:

```text
(customer_id, (order_status, pincode))
```

Important:

Only do this when `customers_dict` fits in driver and executor memory.

## Broadcast Variable

Broadcast variable is read-only data copied to executors.

```python
broadcast_var = spark.sparkContext.broadcast(small_data)
```

Access inside transformations:

```python
broadcast_var.value
```

Use cases:

- Small lookup table.
- Boring words list.
- Configuration map.
- Country code mapping.
- Fraud rule dictionary.

## Practical Pattern 1: Filter -> Map -> Reduce

Use case:

Customers with most CLOSED orders.

```python
result = (
    orders_rdd
    .filter(lambda x: x.split(",")[3] == "CLOSED")
    .map(lambda x: (x.split(",")[2], 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

result.take(10)
```

Why this is good:

- Filters early.
- Reduces data before aggregation.
- Uses `reduceByKey`.

## Practical Pattern 2: Map -> Distinct -> Count

Use case:

Count active customers.

```python
active_customers = (
    orders_rdd
    .map(lambda x: x.split(",")[2])
    .distinct()
    .count()
)
```

Active customer means customer placed at least one order.

## Practical Pattern 3: Join -> Map -> Reduce

Use case:

Revenue by state.

Flow:

```text
orders -> (customer_id, order_id)
customers -> (customer_id, state)
join -> (customer_id, (order_id, state))
map -> (order_id, state)
order_items -> (order_id, subtotal)
join -> (order_id, (state, subtotal))
map -> (state, subtotal)
reduceByKey -> revenue by state
```

## Saving Results

Use:

```python
rdd.saveAsTextFile("/user/<username>/data/output")
```

Common issue:

Output path already exists.

Fix:

```bash
hadoop fs -rm -R /user/<username>/data/output
```

## Common Mistakes

- Joining RDDs without converting to pair RDDs.
- Joining on wrong key type, like string in one RDD and int in another.
- Broadcasting a dataset that is too large.
- Using list scan lookup instead of dictionary lookup for broadcast data.
- Forgetting output directory must not exist.
- Using normal join when broadcast join is better.
- Using broadcast join when both datasets are large.

## Best Practices

- Make join keys same type on both sides.
- Filter columns before join.
- Filter rows before join.
- Broadcast only genuinely small data.
- Use dictionary for broadcast lookups.
- Check Spark UI for shuffle.
- Prefer DataFrame joins in production when possible.

## Interview Questions

### Beginner Questions

- What is RDD join?
- What is pair RDD?
- What is broadcast variable?
- What is broadcast join?
- Why do joins cause shuffle?

### Intermediate Questions

- When should we use broadcast join?
- When should we avoid broadcast join?
- Why should broadcast lookup use dictionary?
- What happens if join key type differs?
- Why is standard join a wide transformation?

### Senior Data Engineer Questions

- How do you optimize large-small joins?
- How do you handle skewed joins?
- How do you decide whether dataset is safe to broadcast?
- What metrics in Spark UI show join cost?
- How would you rewrite an expensive RDD join in DataFrame API?

## Quick Revision

```text
RDD join needs pair RDDs:
(key, value)

Normal join = wide transformation = shuffle

Broadcast join:
small dataset copied to every executor
large dataset stays partitioned
join happens locally

Use broadcast only when small dataset fits memory
```
