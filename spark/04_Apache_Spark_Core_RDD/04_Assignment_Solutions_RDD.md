# Assignment Solutions Using Spark RDD

## Introduction

This assignment asks us to solve business problems using Spark RDDs.

Datasets:

Retail:

```text
/public/ujjawalsingh/retail_db/orders/*
/public/ujjawalsingh/retail_db/customers/*
/public/ujjawalsingh/retail_db/order_items/*
```

Covid:

```text
/public/ujjawalsingh/covid19/cases/covid_dataset_cases.csv
/public/ujjawalsingh/covid19/states/covid_dataset_states.csv
```

Reviews:

```text
/public/ujjawalsingh/reviews/ujjawalsingh-student-reviews.csv
/data/ujjawalsingh/boringwords.txt
```

Important:

These solutions use RDDs because the assignment asks for Spark Core API. In production, I would usually prefer DataFrames/Spark SQL for better optimization and cleaner code.

## SparkSession

```python
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = (
    SparkSession.builder
    .config("spark.ui.port", "0")
    .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()
)
```

## Retail Dataset Schemas

### Orders

```text
order_id, order_date, order_customer_id, order_status
```

### Customers

```text
customer_id, customer_fname, customer_lname, customer_email, customer_password,
customer_street, customer_city, customer_state, customer_zipcode
```

### Order Items

```text
order_item_id, order_id, order_item_product_id, order_item_quantity,
order_item_subtotal, order_item_product_price
```

## Load Retail RDDs

```python
orders_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/orders/*")
customers_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/customers/*")
order_items_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/retail_db/order_items/*")
```

## Q1. Top 10 Customers Who Spent The Most

Goal:

Find premium customers by total amount spent.

Need:

```text
orders: order_id -> customer_id
order_items: order_id -> subtotal
```

Code:

```python
orders_map = orders_rdd.map(
    lambda x: (int(x.split(",")[0]), int(x.split(",")[2]))
)

order_items_map = order_items_rdd.map(
    lambda x: (int(x.split(",")[1]), float(x.split(",")[4]))
)

joined_rdd = order_items_map.join(orders_map)

customer_amount = joined_rdd.map(
    lambda x: (x[1][1], x[1][0])
)

top_customers = (
    customer_amount
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

top_customers.take(10)
```

Explanation:

- Join happens on `order_id`.
- Joined format is `(order_id, (subtotal, customer_id))`.
- Map to `(customer_id, subtotal)`.
- Sum subtotal by customer.
- Sort descending.

## Q2. Top 10 Product IDs With Most Quantity Sold

```python
top_products = (
    order_items_rdd
    .map(lambda x: (int(x.split(",")[2]), int(x.split(",")[3])))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

top_products.take(10)
```

Explanation:

- Product ID is index `2`.
- Quantity is index `3`.
- Sum quantity per product.

## Q3. Customers From Caguas City

```python
caguas_count = (
    customers_rdd
    .map(lambda x: x.split(",")[6])
    .filter(lambda city: city == "Caguas")
    .count()
)

caguas_count
```

Customer city is index `6`.

## Q4. Top 3 States With Maximum Customers

```python
top_states = (
    customers_rdd
    .map(lambda x: (x.split(",")[7], 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

top_states.take(3)
```

Customer state is index `7`.

## Q5. Customers Who Spent More Than 1000 Dollars

```python
orders_map = orders_rdd.map(
    lambda x: (int(x.split(",")[0]), int(x.split(",")[2]))
)

order_items_map = order_items_rdd.map(
    lambda x: (int(x.split(",")[1]), float(x.split(",")[4]))
)

customer_spend = (
    order_items_map
    .join(orders_map)
    .map(lambda x: (x[1][1], x[1][0]))
    .reduceByKey(lambda x, y: x + y)
)

customers_gt_1000 = customer_spend.filter(lambda x: x[1] > 1000)

customers_gt_1000.count()
```

Optimization:

If I need to reuse `customer_spend` for multiple actions, cache it:

```python
customer_spend.cache()
```

## Q6. State With Most CLOSED Orders

Need:

```text
orders: customer_id -> CLOSED status
customers: customer_id -> state
```

Code:

```python
closed_orders = (
    orders_rdd
    .map(lambda x: (int(x.split(",")[2]), x.split(",")[3]))
    .filter(lambda x: x[1] == "CLOSED")
)

customers_state = customers_rdd.map(
    lambda x: (int(x.split(",")[0]), x.split(",")[7])
)

state_closed_count = (
    closed_orders
    .join(customers_state)
    .map(lambda x: (x[1][1], 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

state_closed_count.take(1)
```

Joined format:

```text
(customer_id, (order_status, state))
```

## Q7. Count Active Customers

Active customer means customer placed at least one order.

Better solution:

```python
active_customers = (
    orders_rdd
    .map(lambda x: x.split(",")[2])
    .distinct()
    .count()
)

active_customers
```

Why this is better:

We only need distinct customer IDs. No need to count orders per customer first.

## Q8. Revenue Generated By Each State

Need:

```text
customers: customer_id -> state
orders: customer_id -> order_id
order_items: order_id -> subtotal
```

Code:

```python
orders_customer = orders_rdd.map(
    lambda x: (int(x.split(",")[2]), int(x.split(",")[0]))
)

customers_state = customers_rdd.map(
    lambda x: (int(x.split(",")[0]), x.split(",")[7])
)

customer_order_state = (
    orders_customer
    .join(customers_state)
    .map(lambda x: (x[1][0], x[1][1]))
)

order_subtotal = order_items_rdd.map(
    lambda x: (int(x.split(",")[1]), float(x.split(",")[4]))
)

revenue_by_state = (
    customer_order_state
    .join(order_subtotal)
    .map(lambda x: (x[1][0], x[1][1]))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

revenue_by_state.collect()
```

Flow:

```text
(customer_id, order_id) join (customer_id, state)
    -> (order_id, state)

(order_id, state) join (order_id, subtotal)
    -> (state, subtotal)

reduceByKey -> revenue by state
```

## Covid Dataset Schema Notes

Cases columns include:

```text
date,state,positive,negative,pending,hospitalizedCurrently,
hospitalizedCumulative,inIcuCurrently,inIcuCumulative,
onVentilatorCurrently,onVentilatorCumulative,recovered,...
death,...
total,totalTestResults,...
```

States columns include:

```text
state,notes,covid19Site,covid19SiteSecondary,covid19SiteTertiary,
twitter,covid19SiteOld,name,fips,pui,pum
```

Load:

```python
cases_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/covid19/cases/covid_dataset_cases.csv")
states_rdd = spark.sparkContext.textFile("/public/ujjawalsingh/covid19/states/covid_dataset_states.csv")
```

Note:

If headers exist, remove them before integer conversion.

Safer helper:

```python
def safe_int(value):
    try:
        return int(value)
    except:
        return 0
```

## Covid Q1. Top 10 States With Highest Positive Cases

```python
positive_cases = (
    cases_rdd
    .map(lambda x: (x.split(",")[1], safe_int(x.split(",")[2])))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

positive_cases.take(10)
```

## Covid Q2. Total Count Of People In ICU Currently

```python
icu_count = (
    cases_rdd
    .map(lambda x: safe_int(x.split(",")[7]))
    .sum()
)

icu_count
```

## Covid Q3. Top 15 States Having Maximum Recovery

```python
recovered_rdd = (
    cases_rdd
    .map(lambda x: (x.split(",")[1], safe_int(x.split(",")[11])))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

recovered_rdd.take(15)
```

## Covid Q4. Top 3 States Having Least Deaths

```python
least_deaths = (
    cases_rdd
    .map(lambda x: (x.split(",")[1], safe_int(x.split(",")[23])))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1])
)

least_deaths.take(3)
```

## Covid Q5. Total Hospitalized Currently

```python
hospitalized_currently = (
    cases_rdd
    .map(lambda x: safe_int(x.split(",")[5]))
    .sum()
)

hospitalized_currently
```

## Covid Q6. Twitter Handle And FIPS For Top 15 States By Total Cases

```python
states_mapped = states_rdd.map(
    lambda x: (x.split(",")[0], (x.split(",")[5], safe_int(x.split(",")[8])))
)

total_cases = (
    cases_rdd
    .map(lambda x: (x.split(",")[1], safe_int(x.split(",")[28])))
    .reduceByKey(lambda x, y: x + y)
)

joined_rdd = total_cases.join(states_mapped)

final_rdd = joined_rdd.sortBy(lambda x: x[1][0], ascending=False)

final_rdd.take(15)
```

Output format:

```text
(state, (total_cases, (twitter_handle, fips)))
```

## Reviews Dataset: Top 20 Meaningful Words

Goal:

Find top 20 words in student reviews excluding boring words like:

```text
the, is, are, a
```

Dataset:

```text
/public/ujjawalsingh/reviews/ujjawalsingh-student-reviews.csv
```

Boring words local file:

```text
/data/ujjawalsingh/boringwords.txt
```

## Move Boring Words To HDFS

```bash
hadoop fs -mkdir -p /user/<username>/TT
hadoop fs -put /data/ujjawalsingh/boringwords.txt /user/<username>/TT
```

## Broadcast Boring Words

```python
reviews_rdd = spark.sparkContext.textFile(
    "/public/ujjawalsingh/reviews/ujjawalsingh-student-reviews.csv"
)

boring_words_rdd = spark.sparkContext.textFile(
    f"/user/{username}/TT/boringwords.txt"
)

boring_words = set(boring_words_rdd.collect())
boring_words_broadcast = spark.sparkContext.broadcast(boring_words)
```

Why broadcast:

- Boring words list is small.
- Every executor needs it for filtering.
- Avoids repeatedly sending it with every task.

## Top 20 Words Code

```python
import re

top_words = (
    reviews_rdd
    .flatMap(lambda line: re.split(r"\\W+", line.lower()))
    .filter(lambda word: word != "")
    .filter(lambda word: word not in boring_words_broadcast.value)
    .map(lambda word: (word, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], ascending=False)
)

top_words.take(20)
```

Why this is better than simple `split(" ")`:

- Handles punctuation.
- Converts to lowercase.
- Removes empty words.
- Uses set lookup for fast boring word filtering.

## Assignment Performance Notes

- Use `reduceByKey` instead of `groupByKey`.
- Broadcast small lookup datasets or boring words.
- Cache reused expensive RDDs.
- Filter early.
- Avoid `collect()` except for small lookup/broadcast data.
- Use `take()` for top results.
- Watch joins because they are wide transformations.

## Common Mistakes

- Forgetting to remove headers before `int()`.
- Using simple comma split on fields with embedded commas.
- Broadcasting huge datasets.
- Using list instead of set for boring words lookup.
- Using `groupByKey` for counts.
- Calling `collect()` on large RDD.
- Joining with mismatched key types.

CSV warning:

RDD examples use `split(",")` because course files are simple enough for practice. In production, use DataFrame CSV reader or Python CSV parsing for quoted commas.

## Interview Questions

### Beginner Questions

- How do you load text files as RDD?
- How do you join two RDDs?
- How do you count by key?
- How do you find distinct customers?
- How do you broadcast small data?

### Intermediate Questions

- How do you calculate revenue by state?
- How do you optimize top-N word count?
- Why use broadcast for boring words?
- Why use `set` for lookup?
- Why can RDD `split(",")` be risky?

### Senior Data Engineer Questions

- How would you optimize retail assignment for production?
- How would you handle malformed records?
- How would you avoid multiple joins?
- How would you decide between RDD and DataFrame?
- How would you test these transformations?

## Quick Revision

```text
Retail joins:
orders + order_items -> spend by customer
orders + customers -> state based metrics
orders + customers + order_items -> revenue by state

Covid:
map state with numeric column -> reduceByKey -> sort/take

Reviews:
flatMap words -> remove boring words -> reduceByKey -> top 20

Optimization:
filter early, reduceByKey, broadcast small data, avoid collect
```
