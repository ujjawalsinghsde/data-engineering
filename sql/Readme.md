# SQL for Data Engineers

## 1. SQL Fundamentals

### SQL Basics

**What is it?** SQL is a language for working with relational and analytical data. You ask for the result you want; the engine decides how to execute it.

**Why use it?** In data engineering, SQL is the fastest way to transform large structured data close to where it lives.

**Syntax**

```sql
SELECT column_name
FROM table_name
WHERE condition;
```

**Example**

```sql
SELECT order_id, customer_id, order_date, total_amount
FROM bronze_orders
WHERE ingestion_date = CURRENT_DATE;
```

**Remember**

- SQL usually works on sets of rows, not one row at a time.
- Logical query order is different from written order.
- The engine may optimize execution differently than the query text.

**Common mistakes**

- Thinking SQL executes top to bottom exactly as written.
- Using `SELECT *` in production pipelines.
- Ignoring `NULL` behavior.

### Query Structure

Written order:

```sql
SELECT
FROM
WHERE
GROUP BY
HAVING
ORDER BY
LIMIT;
```

Logical order:

1. `FROM`
2. `JOIN`
3. `WHERE`
4. `GROUP BY`
5. Aggregations
6. `HAVING`
7. `SELECT`
8. `DISTINCT`
9. `ORDER BY`
10. `LIMIT`

### SELECT

**What is it?** Chooses columns or expressions.

**Why use it?** To project only the data needed for downstream transformation.

**Example**

```sql
SELECT
  transaction_id,
  customer_id,
  CAST(transaction_ts AS DATE) AS transaction_date
FROM raw_transactions;
```

**Remember**

- Prefer explicit columns in ETL.
- Expressions can create derived columns.

**Common mistakes**

- `SELECT *` in curated layers.
- Depending on column order instead of names.

### WHERE

**What is it?** Filters rows before aggregation.

**Why use it?** Reduces data early, improves performance, and keeps transformations correct.

**Example**

```sql
SELECT *
FROM events
WHERE event_date >= DATE '2026-10-01'
  AND event_date <  DATE '2026-10-02';
```

**Remember**

- `WHERE` runs before `GROUP BY`.
- Use range filters on partition columns when possible.

**Common mistakes**

- Filtering aggregate results in `WHERE`.
- Writing `WHERE column = NULL`; use `IS NULL`.

### DISTINCT

**What is it?** Removes duplicate result rows.

**Why use it?** Useful for exploration or deduped lists, but expensive on big data.

**Example**

```sql
SELECT DISTINCT source_system, batch_id
FROM ingestion_audit
WHERE load_date = CURRENT_DATE;
```

**Remember**

- `DISTINCT` applies to the full selected row.
- It can hide data quality issues.

**Common mistakes**

- Using `DISTINCT` to fix joins without understanding why duplicates appeared.

### ORDER BY

**What is it?** Sorts output rows.

**Why use it?** Required when order matters, such as latest records or deterministic exports.

**Example**

```sql
SELECT *
FROM pipeline_runs
ORDER BY run_started_at DESC;
```

**Remember**

- Tables are unordered unless you specify `ORDER BY`.
- Sorting large data is expensive.

**Common mistakes**

- Assuming `LIMIT 1` returns the latest row without `ORDER BY`.

### LIMIT

**What is it?** Restricts number of returned rows.

**Why use it?** Fast inspection during development.

```sql
SELECT *
FROM customers
LIMIT 10;
```

**Dialect notes**

| Database | Syntax |
|---|---|
| PostgreSQL, MySQL, Redshift, Databricks | `LIMIT 10` |
| SQL Server | `SELECT TOP 10 ...` or `OFFSET/FETCH` |
| Oracle | `FETCH FIRST 10 ROWS ONLY` |

**Common mistakes**

- Using `LIMIT` in production logic without deterministic ordering.

### Aliases

**What is it?** Temporary names for columns or tables.

**Why use it?** Improves readability, especially in joins and transformations.

```sql
SELECT customer_id AS id
FROM customers;
```

```sql
SELECT c.customer_id, o.order_id
FROM customers AS c
JOIN orders AS o
  ON c.customer_id = o.customer_id;
```

### Operators

Common operators:

| Type | Operators |
|---|---|
| Comparison | `=`, `<>`, `!=`, `>`, `>=`, `<`, `<=` |
| Logical | `AND`, `OR`, `NOT` |
| Pattern | `LIKE`, `ILIKE` in PostgreSQL/Databricks |
| Set membership | `IN`, `NOT IN` |
| Range | `BETWEEN` |
| Null checks | `IS NULL`, `IS NOT NULL` |

```sql
SELECT *
FROM orders
WHERE total_amount >= 100
  AND order_status IN ('COMPLETE', 'SHIPPED');
```

### CASE WHEN

**What is it?** SQL's conditional expression.

**Why use it?** Creates business rules directly in transformations.

```sql
CASE
  WHEN condition THEN result
  ELSE fallback
END
```

**Example**

```sql
SELECT
  customer_id,
  CASE
    WHEN email IS NULL THEN 'missing_email'
    WHEN email NOT LIKE '%@%' THEN 'invalid_email'
    ELSE 'valid'
  END AS email_quality_status
FROM customers;
```

**Common mistakes**

- Forgetting `ELSE`, causing unexpected `NULL`.
- Overlapping conditions in the wrong order.

### NULL

**What is it?** Unknown or missing value. It is not zero, empty string, or false.

**Why care?** `NULL` silently changes filter, join, aggregation, and comparison behavior.

```sql
SELECT *
FROM customers
WHERE phone IS NULL;
```

**Remember**

- `NULL = NULL` is not true.
- `COUNT(column)` ignores `NULL`.
- `COUNT(*)` counts rows.
- `SUM`, `AVG`, `MIN`, `MAX` ignore `NULL`.

**Common mistakes**

- `WHERE col = NULL`
- Assuming `NOT IN` is safe when subquery returns `NULL`.

### COALESCE

**What is it?** Returns the first non-null value.

**Why use it?** Gives fallback values during cleansing and reporting.

**Example**

```sql
SELECT
  order_id,
  COALESCE(discount_amount, 0) AS discount_amount
FROM orders;
```

**Common mistakes**

- Replacing `NULL` with `0` when missing really means unknown.

### CAST / Type Conversion

**What is it?** Converts one data type into another.

**Why use it?** Raw data often arrives as strings; curated tables need correct types.

```sql
SELECT CAST('2026-10-05' AS DATE) AS order_date;
```

**Example**

```sql
SELECT
  CAST(order_id AS BIGINT) AS order_id,
  CAST(order_ts AS TIMESTAMP) AS order_ts,
  CAST(total_amount AS DECIMAL(18,2)) AS total_amount
FROM raw_orders;
```

**Dialect notes**

| Concept | PostgreSQL | SQL Server | Databricks |
|---|---|---|---|
| Safe cast | no built-in `TRY_CAST` in old versions | `TRY_CAST` | `try_cast` |
| Date literal | `DATE '2026-10-05'` | `CAST('2026-10-05' AS date)` | `DATE '2026-10-05'` |

**Common mistakes**

- Casting after filtering when pushdown could have used the raw partition column.
- Losing precision by casting money to float.

**Key Takeaways**

- SQL is set-based.
- Know logical query order.
- `NULL`, `CAST`, and `CASE` show up in real pipelines constantly.

**Common Mistakes**

- `SELECT *` everywhere.
- `= NULL`.
- Using `DISTINCT` as a bandage.

**Interview Focus**

- Explain logical query order.
- Explain `COUNT(*)` vs `COUNT(col)`.
- Explain `WHERE` vs `HAVING`.

## 2. Filtering and Conditions

Filtering is where you decide which rows are allowed into the transformation. In production pipelines, a bad filter can silently drop revenue, double count users, or break incremental loads.

### Comparison Operators

```sql
SELECT *
FROM transactions
WHERE amount > 0
  AND transaction_status <> 'FAILED';
```

Use comparisons for quality rules, business filters, and incremental boundaries.

### AND / OR / NOT

```sql
SELECT *
FROM orders
WHERE order_status = 'COMPLETE'
  AND (country = 'US' OR country = 'CA')
  AND NOT is_test_order;
```

Parentheses matter. SQL evaluates `AND` before `OR`.

### IN

```sql
SELECT *
FROM events
WHERE event_name IN ('signup', 'purchase', 'refund');
```

Use `IN` for readable equality filters. Use a lookup table instead of a huge hard-coded list in production.

### BETWEEN

```sql
SELECT *
FROM orders
WHERE order_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';
```

`BETWEEN` is inclusive. For timestamps, prefer half-open intervals:

```sql
WHERE order_ts >= TIMESTAMP '2026-10-01 00:00:00'
  AND order_ts <  TIMESTAMP '2026-11-01 00:00:00'
```

### LIKE

```sql
SELECT *
FROM customers
WHERE email LIKE '%@gmail.com';
```

`%` means any characters. `_` means one character.

Dialect note: PostgreSQL and Databricks support `ILIKE` for case-insensitive matching. SQL Server case sensitivity depends on collation.

### IS NULL / IS NOT NULL

```sql
SELECT *
FROM customers
WHERE email IS NOT NULL;
```

Use this for completeness checks and source quality validation.

### Complex Filtering

```sql
SELECT *
FROM transactions
WHERE transaction_ts >= TIMESTAMP '2026-10-01 00:00:00'
  AND transaction_ts <  TIMESTAMP '2026-10-02 00:00:00'
  AND status = 'SUCCESS'
  AND amount > 0
  AND (
    payment_method IN ('card', 'upi')
    OR source_system = 'partner_api'
  );
```

**Key Takeaways**

- Filter early.
- Use parentheses for mixed `AND` / `OR`.
- Use half-open timestamp ranges.

**Common Mistakes**

- `BETWEEN` on timestamps and missing late-night records.
- `NOT IN` with nullable subqueries.
- Functions on partition columns that block pruning.

**Interview Focus**

- `IN` vs `EXISTS`.
- Handling `NULL` in filters.
- Date range filtering for incremental loads.

## 3. Aggregations

Aggregations turn many rows into fewer rows. For data engineers, this is the foundation of marts, metrics, validations, reconciliation, and reporting tables.

### Core Functions

```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(customer_id) AS non_null_customers,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_order_value,
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date
FROM orders;
```

### GROUP BY

```sql
SELECT
  customer_id,
  COUNT(*) AS order_count,
  SUM(total_amount) AS lifetime_revenue
FROM orders
GROUP BY customer_id;
```

Every non-aggregated selected column must be in the `GROUP BY` in standard SQL.

### HAVING

```sql
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 5;
```

`WHERE` filters rows before grouping. `HAVING` filters groups after aggregation.

### Conditional Aggregation

```sql
SELECT
  customer_id,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) AS completed_orders,
  SUM(CASE WHEN status = 'CANCELLED' THEN total_amount ELSE 0 END) AS cancelled_amount
FROM orders
GROUP BY customer_id;
```

Useful for metrics in one pass.

### COUNT(*) vs COUNT(column)

| Expression | Meaning |
|---|---|
| `COUNT(*)` | Count all rows |
| `COUNT(column)` | Count rows where column is not null |
| `COUNT(DISTINCT column)` | Count unique non-null values |

**Key Takeaways**

- Aggregation changes row grain.
- Always know the output grain: customer, day, product, customer-day, etc.
- Conditional aggregation is heavily used in marts and DQ checks.

**Common Mistakes**

- Selecting columns not in `GROUP BY`.
- Filtering aggregate output in `WHERE`.
- Aggregating after a join that changed the row count.

**Interview Focus**

- `WHERE` vs `HAVING`.
- `COUNT(*)` vs `COUNT(col)`.
- How joins affect aggregates.

## 4. JOINS ⭐

Joins combine rows from related tables. In data engineering, joins are everywhere: facts to dimensions, source to lookup, staging to target, transactions to customers, events to sessions.

### Sample Tables

`customers`

| customer_id | name |
|---:|---|
| 1 | Asha |
| 2 | Ben |
| 3 | Chen |

`orders`

| order_id | customer_id | amount |
|---:|---:|---:|
| 101 | 1 | 50 |
| 102 | 1 | 80 |
| 103 | 2 | 30 |
| 104 | 4 | 20 |

### INNER JOIN

Keeps only matching rows.

```sql
SELECT c.customer_id, c.name, o.order_id, o.amount
FROM customers c
INNER JOIN orders o
  ON c.customer_id = o.customer_id;
```

Output:

| customer_id | name | order_id | amount |
|---:|---|---:|---:|
| 1 | Asha | 101 | 50 |
| 1 | Asha | 102 | 80 |
| 2 | Ben | 103 | 30 |

### LEFT JOIN

Keeps all rows from the left table, plus matches from the right.

```sql
SELECT c.customer_id, c.name, o.order_id
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id;
```

Use it for enrichment and finding missing records.

### RIGHT JOIN

Keeps all rows from the right table. Usually rewrite as `LEFT JOIN` by swapping table order because it is easier to read.

### FULL OUTER JOIN

Keeps all rows from both sides.

```sql
SELECT c.customer_id AS customer_customer_id, o.customer_id AS order_customer_id
FROM customers c
FULL OUTER JOIN orders o
  ON c.customer_id = o.customer_id;
```

Useful for reconciliation.

### CROSS JOIN

Every row from left paired with every row from right.

```sql
SELECT d.calendar_date, p.product_id
FROM calendar d
CROSS JOIN products p;
```

Useful to build dense date-product grids. Dangerous if both sides are large.

### SELF JOIN

Join a table to itself.

```sql
SELECT e.employee_id, e.name, m.name AS manager_name
FROM employees e
LEFT JOIN employees m
  ON e.manager_id = m.employee_id;
```

### Multiple Joins

```sql
SELECT
  o.order_id,
  c.customer_name,
  p.product_name,
  oi.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id;
```

### Join Cardinality

| Relationship | Meaning | Risk |
|---|---|---|
| One-to-one | one matching row each side | usually safe |
| One-to-many | one customer, many orders | expected row multiplication |
| Many-to-many | many matches both sides | often accidental explosion |

### Why Duplicate Rows Appear After Joins

Duplicates usually appear because the join key is not unique on one or both sides.

Example:

`customers` has one row per customer, but `orders` has many rows per customer. Joining customer to orders returns one row per order, not one row per customer.

Check it:

```sql
SELECT customer_id, COUNT(*) AS rows_per_key
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

### How to Identify Join Duplicate Causes

```sql
SELECT customer_id, COUNT(*) AS cnt
FROM dimension_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Check both sides. Then check whether you are missing part of a composite key:

```sql
-- Bad if product_id is only unique within source_system
ON s.product_id = p.product_id

-- Better
ON s.source_system = p.source_system
AND s.product_id = p.product_id
```

### How to Prevent Unwanted Duplicates

- Join on the full business key.
- Deduplicate dimensions before joining.
- Aggregate the many-side before joining when you need one row per key.
- Use `ROW_NUMBER()` to choose the latest active dimension row.
- Validate row counts before and after important joins.

```sql
WITH latest_customer AS (
  SELECT *
  FROM (
    SELECT
      c.*,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY updated_at DESC
      ) AS rn
    FROM customer_dim c
  ) x
  WHERE rn = 1
)
SELECT o.order_id, c.customer_segment
FROM orders o
LEFT JOIN latest_customer c
  ON o.customer_id = c.customer_id;
```

### Handling NULLs in Joins

`NULL` does not equal `NULL`.

```sql
-- Usually preferred
ON a.customer_id = b.customer_id

-- Use null-safe logic only when business rules truly require it
ON COALESCE(a.customer_id, -1) = COALESCE(b.customer_id, -1)
```

Null-safe equality differs by dialect:

| Database | Null-safe equality |
|---|---|
| PostgreSQL | `IS NOT DISTINCT FROM` |
| MySQL | `<=>` |
| Databricks | `<=>` |
| SQL Server | explicit logic |

### Finding Unmatched Records

```sql
SELECT s.*
FROM staging_orders s
LEFT JOIN dim_customer c
  ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

This is a classic referential integrity check.

**Key Takeaways**

- Always know the grain before and after the join.
- Duplicates are usually cardinality problems.
- Validate uniqueness of join keys.

**Common Mistakes**

- Joining facts to non-unique dimensions.
- Filtering right-table columns in `WHERE` after a `LEFT JOIN`, accidentally making it an inner join.
- Joining on incomplete keys.

**Interview Focus**

- Explain duplicate rows after joins.
- Find unmatched records.
- One-to-many vs many-to-many joins.

## 5. Subqueries

A subquery is a query inside another query. Use subqueries when you need an intermediate result but do not want to create a physical table.

### Scalar Subquery

Returns one value.

```sql
SELECT *
FROM orders
WHERE total_amount > (SELECT AVG(total_amount) FROM orders);
```

### Subquery with IN

```sql
SELECT *
FROM customers
WHERE customer_id IN (
  SELECT customer_id
  FROM orders
  WHERE order_date >= DATE '2026-10-01'
);
```

### Subquery with EXISTS

```sql
SELECT *
FROM customers c
WHERE EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.customer_id
);
```

`EXISTS` checks whether at least one matching row exists.

### Correlated Subquery

References the outer query.

```sql
SELECT *
FROM orders o
WHERE order_date = (
  SELECT MAX(o2.order_date)
  FROM orders o2
  WHERE o2.customer_id = o.customer_id
);
```

Often replace with window functions for clarity and speed.

### Subquery in FROM

```sql
SELECT customer_id, total_spend
FROM (
  SELECT customer_id, SUM(total_amount) AS total_spend
  FROM orders
  GROUP BY customer_id
) s
WHERE total_spend > 1000;
```

### Subquery in SELECT

```sql
SELECT
  c.customer_id,
  c.customer_name,
  (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count
FROM customers c;
```

This can be expensive for large tables.

### IN vs EXISTS

| Use | Better choice |
|---|---|
| Small static list | `IN` |
| Existence check against large table | `EXISTS` |
| Nullable subquery column | `EXISTS` is safer |

### Subquery vs JOIN

Use a join when you need columns from both tables. Use `EXISTS` when you only need to check existence. Use CTEs when the logic needs naming and readability.

**Key Takeaways**

- Subqueries are useful, but CTEs often read better.
- `EXISTS` avoids `NULL` traps from `IN`.
- Window functions often beat correlated subqueries for ranking/latest-row problems.

**Common Mistakes**

- Correlated subqueries over huge tables without understanding cost.
- `NOT IN` with nullable subquery results.

**Interview Focus**

- `IN` vs `EXISTS`.
- Subquery vs join.
- Correlated subquery behavior.

## 6. Set Operations

Set operations combine results vertically. Columns must align by count and compatible type.

### UNION

Combines rows and removes duplicates.

```sql
SELECT customer_id FROM web_customers
UNION
SELECT customer_id FROM store_customers;
```

### UNION ALL

Combines rows and keeps duplicates.

```sql
SELECT * FROM orders_2026_09
UNION ALL
SELECT * FROM orders_2026_10;
```

Use `UNION ALL` for pipeline appends unless deduplication is required.

### INTERSECT

Rows present in both result sets.

```sql
SELECT customer_id FROM crm_customers
INTERSECT
SELECT customer_id FROM billing_customers;
```

### EXCEPT / MINUS

Rows in the first query but not the second.

```sql
SELECT order_id FROM source_orders
EXCEPT
SELECT order_id FROM target_orders;
```

Oracle uses `MINUS`. PostgreSQL, SQL Server, Redshift, and Databricks support `EXCEPT`.

**Key Takeaways**

- Use `UNION ALL` by default for performance.
- Use `EXCEPT` for reconciliation.
- Column order matters.

**Common Mistakes**

- Using `UNION` accidentally and removing valid duplicate records.
- Mismatched column meanings with the same data types.

**Interview Focus**

- `UNION` vs `UNION ALL`.
- Source-target validation with `EXCEPT`.

## 7. String Functions

String functions clean identifiers, emails, names, codes, and semi-structured extracts.

| Function | Purpose | Example |
|---|---|---|
| `CONCAT` | combine strings | `CONCAT(first_name, ' ', last_name)` |
| `SUBSTRING` | extract part | `SUBSTRING(order_code, 1, 3)` |
| `LENGTH` | string length | `LENGTH(email)` |
| `UPPER` | uppercase | `UPPER(country_code)` |
| `LOWER` | lowercase | `LOWER(email)` |
| `TRIM` | remove spaces | `TRIM(product_code)` |
| `REPLACE` | replace text | `REPLACE(phone, '-', '')` |
| `SPLIT` | split text | dialect-specific |
| Pattern matching | validate/search | `LIKE`, regex functions |

```sql
SELECT
  LOWER(TRIM(email)) AS normalized_email,
  REPLACE(phone, '-', '') AS clean_phone
FROM raw_customers;
```

### SPLIT Examples

Databricks/PostgreSQL style:

```sql
SELECT SPLIT(email, '@')[1] AS email_domain
FROM customers;
```

SQL Server:

```sql
SELECT PARSENAME(REPLACE(email, '@', '.'), 1) AS email_domain
FROM customers;
```

### Pattern Matching

```sql
SELECT *
FROM customers
WHERE LOWER(email) LIKE '%@gmail.com';
```

Regex differs strongly by database:

| Database | Regex |
|---|---|
| PostgreSQL | `~`, `regexp_replace` |
| MySQL | `REGEXP` |
| SQL Server | limited native regex |
| Databricks | `rlike`, `regexp_extract`, `regexp_replace` |

**Key Takeaways**

- Normalize strings before joining.
- Trim raw text fields.
- Use regex carefully; dialects differ.

**Common Mistakes**

- Joining on untrimmed codes.
- Case-sensitive comparisons when source systems vary.

**Interview Focus**

- Clean email/domain fields.
- Standardize product/customer codes.

## 8. Date and Time

Date handling is core data engineering. Partitions, watermarks, SLAs, incremental loads, and metrics all depend on time.

### DATE vs TIMESTAMP

`DATE` stores calendar date. `TIMESTAMP` stores date and time.

```sql
SELECT
  CAST(order_ts AS DATE) AS order_date,
  order_ts
FROM orders;
```

### Extracting Year / Month / Day

```sql
SELECT
  EXTRACT(YEAR FROM order_date) AS order_year,
  EXTRACT(MONTH FROM order_date) AS order_month
FROM orders;
```

Databricks also supports `year(order_date)`, `month(order_date)`, `day(order_date)`.

### Date Arithmetic

```sql
SELECT order_date + INTERVAL '7 days' AS expected_delivery_date
FROM orders;
```

SQL Server:

```sql
SELECT DATEADD(day, 7, order_date) AS expected_delivery_date
FROM orders;
```

### Date Difference

```sql
SELECT order_id, delivery_date - order_date AS delivery_days
FROM orders;
```

Databricks:

```sql
SELECT datediff(delivery_date, order_date) AS delivery_days
FROM orders;
```

### Date Truncation

```sql
SELECT DATE_TRUNC('month', order_date) AS order_month
FROM orders;
```

Useful for monthly aggregation.

### Formatting Dates

Formatting is usually for final presentation, not storage.

```sql
SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month
FROM orders;
```

Databricks uses `date_format`; PostgreSQL uses `to_char`; SQL Server uses `FORMAT` or `CONVERT`.

### Time Zones

Store timestamps in UTC when possible. Convert to local time for reporting.

```sql
-- Databricks
SELECT from_utc_timestamp(event_ts_utc, 'Asia/Kolkata') AS event_ts_ist
FROM events;
```

### Current Date / Time

```sql
SELECT CURRENT_DATE, CURRENT_TIMESTAMP;
```

### Practical Examples

Incremental load:

```sql
SELECT *
FROM source_orders
WHERE updated_at >  (SELECT last_watermark FROM etl_watermarks WHERE pipeline_name = 'orders')
  AND updated_at <= CURRENT_TIMESTAMP;
```

Daily partition:

```sql
SELECT *
FROM events
WHERE event_date = DATE '2026-10-05';
```

Monthly aggregation:

```sql
SELECT DATE_TRUNC('month', order_date) AS month_start, SUM(total_amount) AS revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date);
```

Last 7 days:

```sql
SELECT *
FROM events
WHERE event_date >= CURRENT_DATE - INTERVAL '7 days';
```

**Key Takeaways**

- Use half-open intervals for timestamps.
- Store UTC, report local.
- Partition filters should be simple and explicit.

**Common Mistakes**

- Casting partition columns in filters.
- Confusing event time with ingestion time.
- Using formatted strings for date logic.

**Interview Focus**

- Incremental loads.
- Watermarks.
- Daily and monthly aggregations.

## 9. CTEs

A CTE, or common table expression, is a named temporary result inside a query.

### Why Use CTEs?

CTEs make ETL SQL readable. Instead of one giant nested query, you name the steps: raw filter, dedupe, enrich, aggregate, final.

### Basic CTE

```sql
WITH completed_orders AS (
  SELECT *
  FROM orders
  WHERE status = 'COMPLETE'
)
SELECT customer_id, COUNT(*) AS order_count
FROM completed_orders
GROUP BY customer_id;
```

### Multiple CTEs and Chaining

```sql
WITH cleaned AS (
  SELECT
    order_id,
    customer_id,
    CAST(order_ts AS DATE) AS order_date,
    CAST(total_amount AS DECIMAL(18,2)) AS total_amount
  FROM raw_orders
),
valid_orders AS (
  SELECT *
  FROM cleaned
  WHERE customer_id IS NOT NULL
    AND total_amount >= 0
),
daily_revenue AS (
  SELECT order_date, SUM(total_amount) AS revenue
  FROM valid_orders
  GROUP BY order_date
)
SELECT *
FROM daily_revenue;
```

### Recursive CTE

Used for hierarchies or sequences.

```sql
WITH RECURSIVE org AS (
  SELECT employee_id, manager_id, employee_name, 1 AS level
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  SELECT e.employee_id, e.manager_id, e.employee_name, o.level + 1
  FROM employees e
  JOIN org o
    ON e.manager_id = o.employee_id
)
SELECT *
FROM org;
```

Databricks support for recursive CTEs depends on runtime/version. In Spark, recursive hierarchy logic is often handled with iterative processing.

### CTE vs Subquery vs Temporary Table

| Option | Use when |
|---|---|
| CTE | readability inside one query |
| Subquery | simple one-off nested logic |
| Temp table | reused across multiple queries or need materialization |

**Key Takeaways**

- CTEs help write ETL as readable steps.
- They do not always materialize; optimizer may inline them.
- Use meaningful names.

**Common Mistakes**

- Building 20 CTEs with unclear grain changes.
- Reusing the same expensive CTE many times without checking execution behavior.

**Interview Focus**

- CTE vs temp table.
- CTEs for deduplication and staged transformations.

## 10. WINDOW FUNCTIONS ⭐

Window functions calculate across related rows without collapsing rows like `GROUP BY`.

Think of `GROUP BY` as "make fewer rows." Think of window functions as "keep the rows, but add context."

### Core Syntax

```sql
function_name(...) OVER (
  PARTITION BY group_columns
  ORDER BY sort_columns
  ROWS BETWEEN ...
)
```

### OVER(), PARTITION BY, ORDER BY

```sql
SELECT
  customer_id,
  order_id,
  order_date,
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_date DESC
  ) AS order_rank
FROM orders;
```

`PARTITION BY` creates groups. `ORDER BY` decides order insiEach group.

### Ranking Functions

```sql
SELECT
  department_id,
  employee_id,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num,
  RANK()       OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank_num,
  DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dense_rank_num
FROM employees;
```

| Function | Ties | Gaps |
|---|---|---|
| `ROW_NUMBER` | breaks ties arbitrarily unless tie-breaker added | no |
| `RANK` | same rank for ties | yes |
| `DENSE_RANK` | same rank for ties | no |

### NTILE

```sql
SELECT
  customer_id,
  lifetime_value,
  NTILE(4) OVER (ORDER BY lifetime_value DESC) AS value_quartile
FROM customer_metrics;
```

### LAG / LEAD

```sql
SELECT
  customer_id,
  transaction_ts,
  amount,
  LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_ts) AS previous_amount,
  LEAD(amount) OVER (PARTITION BY customer_id ORDER BY transaction_ts) AS next_amount
FROM transactions;
```

### FIRST_VALUE / LAST_VALUE

```sql
SELECT
  customer_id,
  order_id,
  order_date,
  FIRST_VALUE(order_date) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
  ) AS first_order_date
FROM orders;
```

`LAST_VALUE` often needs an explicit frame:

```sql
LAST_VALUE(order_date) OVER (
  PARTITION BY customer_id
  ORDER BY order_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
```

### Running Total

```sql
SELECT
  order_date,
  revenue,
  SUM(revenue) OVER (
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_revenue
FROM daily_revenue;
```

### Moving Average

```sql
SELECT
  order_date,
  revenue,
  AVG(revenue) OVER (
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS seven_day_avg_revenue
FROM daily_revenue;
```

### Window Frames

| Frame | Meaning |
|---|---|
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | running calculation |
| `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` | 7-row moving window |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | whole partition |

### Problem: Latest Record per Customer

**Problem**: For each customer, keep the latest profile record.

**Think through it**: Partition by customer. Sort newest first. Keep row 1.

```sql
WITH ranked AS (
  SELECT
    customer_id,
    email,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY updated_at DESC
    ) AS rn
  FROM customer_profile_staging
)
SELECT customer_id, email, updated_at
FROM ranked
WHERE rn = 1;
```

**Line by line**

- `PARTITION BY customer_id`: rank records separately for each customer.
- `ORDER BY updated_at DESC`: newest record becomes first.
- `ROW_NUMBER`: assigns one winner.
- `WHERE rn = 1`: keeps latest record.

**Common mistakes**

- Missing tie-breaker when two records have same `updated_at`.
- Using `MAX(updated_at)` then joining back and getting duplicates.

### Problem: Second Highest Salary

```sql
WITH ranked AS (
  SELECT
    employee_id,
    salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_rank
  FROM employees
)
SELECT employee_id, salary
FROM ranked
WHERE salary_rank = 2;
```

Use `DENSE_RANK` if ties should share rank.

### Problem: Top 3 Employees per Department

```sql
WITH ranked AS (
  SELECT
    department_id,
    employee_id,
    salary,
    DENSE_RANK() OVER (
      PARTITION BY department_id
      ORDER BY salary DESC
    ) AS rnk
  FROM employees
)
SELECT *
FROM ranked
WHERE rnk <= 3;
```

### Problem: Remove Duplicates

```sql
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY source_system, order_id
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM bronze_orders
)
SELECT *
FROM deduped
WHERE rn = 1;
```

### Problem: Month-over-Month Growth

```sql
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', order_date) AS month_start,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY DATE_TRUNC('month', order_date)
),
with_previous AS (
  SELECT
    month_start,
    revenue,
    LAG(revenue) OVER (ORDER BY month_start) AS previous_month_revenue
  FROM monthly
)
SELECT
  month_start,
  revenue,
  previous_month_revenue,
  (revenue - previous_month_revenue) / NULLIF(previous_month_revenue, 0) AS mom_growth_rate
FROM with_previous;
```

### Problem: Running Total

```sql
SELECT
  order_date,
  revenue,
  SUM(revenue) OVER (
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_revenue
FROM daily_revenue;
```

### Problem: Previous Transaction

```sql
SELECT
  customer_id,
  transaction_id,
  transaction_ts,
  LAG(transaction_ts) OVER (
    PARTITION BY customer_id
    ORDER BY transaction_ts
  ) AS previous_transaction_ts
FROM transactions;
```

### Problem: Next Transaction

```sql
SELECT
  customer_id,
  transaction_id,
  transaction_ts,
  LEAD(transaction_ts) OVER (
    PARTITION BY customer_id
    ORDER BY transaction_ts
  ) AS next_transaction_ts
FROM transactions;
```

**Key Takeaways**

- Window functions preserve row-level detail.
- Always define partition and ordering intentionally.
- Add tie-breakers for deterministic results.

**Common Mistakes**

- Using `ROW_NUMBER` without stable `ORDER BY`.
- Forgetting explicit frame for `LAST_VALUE`.
- Confusing `GROUP BY` with window functions.

**Interview Focus**

- Latest record per key.
- Deduplication.
- Top N per group.
- MoM, running totals, previous/next event.

## 11. Data Transformation

Data transformation is where raw data becomes useful data. In ETL/ELT, SQL transformations usually do four things: clean, standardize, enrich, and reshape.

### Conditional Transformations

```sql
SELECT
  order_id,
  CASE
    WHEN total_amount < 0 THEN 'invalid'
    WHEN status IS NULL THEN 'missing_status'
    ELSE 'valid'
  END AS validation_status
FROM staging_orders;
```

### Deduplication

```sql
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY updated_at DESC, ingestion_ts DESC
    ) AS rn
  FROM staging_orders
)
SELECT *
FROM ranked
WHERE rn = 1;
```

### Data Cleansing

```sql
SELECT
  customer_id,
  LOWER(TRIM(email)) AS email,
  UPPER(TRIM(country_code)) AS country_code,
  NULLIF(TRIM(phone), '') AS phone
FROM raw_customers;
```

### Standardization

```sql
SELECT
  product_id,
  CASE
    WHEN UPPER(category) IN ('MOBILE', 'PHONE', 'SMARTPHONE') THEN 'PHONE'
    WHEN UPPER(category) IN ('TV', 'TELEVISION') THEN 'TV'
    ELSE 'OTHER'
  END AS standard_category
FROM raw_products;
```

### Pivot

Turns rows into columns.

```sql
SELECT
  customer_id,
  SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) AS web_amount,
  SUM(CASE WHEN channel = 'store' THEN amount ELSE 0 END) AS store_amount
FROM sales
GROUP BY customer_id;
```

### Unpivot

Turns columns into rows. ANSI style often uses `UNION ALL`.

```sql
SELECT customer_id, 'web' AS channel, web_amount AS amount FROM customer_channel_sales
UNION ALL
SELECT customer_id, 'store' AS channel, store_amount AS amount FROM customer_channel_sales;
```

### Complex Transformation Pattern

```sql
WITH cleaned AS (
  SELECT
    order_id,
    customer_id,
    CAST(order_ts AS TIMESTAMP) AS order_ts,
    COALESCE(CAST(total_amount AS DECIMAL(18,2)), 0) AS total_amount,
    UPPER(TRIM(status)) AS status
  FROM raw_orders
),
valid AS (
  SELECT *
  FROM cleaned
  WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
),
deduped AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_ts DESC) AS rn
    FROM valid
  ) x
  WHERE rn = 1
)
SELECT *
FROM deduped;
```

**Key Takeaways**

- Transformations should be readable and testable.
- CTEs make pipeline SQL easier to maintain.
- Deduplication needs a business rule for "winner."

**Common Mistakes**

- Removing duplicates without deterministic ordering.
- Replacing missing values without business agreement.

**Interview Focus**

- Clean raw records into curated records.
- Explain deduplication logic.
- Pivot/unpivot use cases.

## 12. DDL and DML

DDL defines objects. DML changes data.

### DDL

```sql
CREATE TABLE customers (
  customer_id BIGINT,
  email VARCHAR(255),
  created_at TIMESTAMP
);

ALTER TABLE customers ADD COLUMN customer_status VARCHAR(50);

DROP TABLE old_customers;

TRUNCATE TABLE staging_orders;
```

`TRUNCATE` removes all rows quickly. It is usually not row-logged like `DELETE`, and rollback behavior depends on database.

### DML

```sql
INSERT INTO customers (customer_id, email, created_at)
VALUES (1, 'asha@example.com', CURRENT_TIMESTAMP);

UPDATE customers
SET email = LOWER(email)
WHERE email <> LOWER(email);

DELETE FROM customers
WHERE customer_id IS NULL;
```

### MERGE ⭐

`MERGE` is critical in data engineering because pipelines often need upserts: update existing records and insert new ones.

```sql
MERGE INTO dim_customer AS tgt
USING staging_customer AS src
  ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.email = src.email,
    tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, email, updated_at)
  VALUES (src.customer_id, src.email, src.updated_at);
```

### Insert-only

```sql
INSERT INTO fact_orders
SELECT *
FROM staging_orders s
WHERE NOT EXISTS (
  SELECT 1
  FROM fact_orders f
  WHERE f.order_id = s.order_id
);
```

### MERGE Dialect Notes

| Database | Notes |
|---|---|
| Databricks / Delta Lake | Strong MERGE support; common for upserts |
| SQL Server | Supports `MERGE`, but many teams prefer separate statements due to historical bugs |
| Oracle | Mature `MERGE` support |
| PostgreSQL | Uses `INSERT ... ON CONFLICT` for many upserts; recent versions support `MERGE` |
| Redshift | Supports `MERGE` in modern Redshift |
| MySQL | `INSERT ... ON DUPLICATE KEY UPDATE` |

**Key Takeaways**

- DDL changes shape; DML changes rows.
- `MERGE` is the standard pattern for upsert pipelines.
- Always dedupe source data before merge if multiple source rows can match one target row.

**Common Mistakes**

- Merging duplicate source keys into one target key.
- Running `DELETE` or `TRUNCATE` without validating environment.
- Forgetting transaction boundaries.

**Interview Focus**

- Explain upsert.
- Write MERGE for incremental pipeline.
- Difference between `DELETE`, `TRUNCATE`, and `DROP`.

## 13. Data Modeling

Data modeling decides how data is shaped for storage, querying, and business meaning.

### Keys and Constraints

| Concept | Meaning | Example |
|---|---|---|
| Primary key | unique identifier for a row | `customer_id` |
| Foreign key | reference to another table | `orders.customer_id` -> `customers.customer_id` |
| Constraint | rule enforced by database | not null, unique, check |

In lakehouses, constraints may be informational or selectively enforced depending on platform.

### Normalization

Break data into smaller related tables to reduce duplication. Common in OLTP.

Example: customers, orders, products are separate.

### Denormalization

Combine data for faster analytical reads. Common in OLAP.

Example: a `fact_order_sales` table includes customer segment and product category.

### OLTP vs OLAP

| OLTP | OLAP |
|---|---|
| operational systems | analytics systems |
| many small writes | large reads/scans |
| normalized | dimensional/denormalized |
| current state | historical analysis |

### Star Schema

Central fact table connected to dimensions.

`fact_sales`: order_id, customer_key, product_key, date_key, amount  
`dim_customer`: customer_key, customer_id, segment  
`dim_product`: product_key, product_id, category  
`dim_date`: date_key, date, month, year

### Snowflake Schema

Dimensions are further normalized.

Example: `dim_product` links to `dim_category`.

### Fact Tables

Store measurable business events.

Examples: orders, transactions, clicks, shipments.

### Dimension Tables

Store descriptive context.

Examples: customers, products, stores, dates.

### Surrogate Key vs Natural Key

| Key | Meaning |
|---|---|
| Natural key | business/source identifier like `customer_id` |
| Surrogate key | warehouse-generated key like `customer_key` |

Surrogate keys are very useful for SCD Type 2.

**Key Takeaways**

- Facts are events; dimensions are descriptions.
- Star schemas are interview-critical.
- Grain is the most important modeling decision.

**Common Mistakes**

- Not defining fact table grain.
- Mixing multiple grains in one table.
- Using mutable natural keys as if they never change.

**Interview Focus**

- Star schema design.
- Fact vs dimension.
- Surrogate key for SCD.

## 14. Slowly Changing Dimensions

SCDs handle changes in dimension attributes over time.

### Why We Need SCD

Customers move cities, products change categories, employees change departments. Analytics often needs either the current value or the historical value at the time of the event.

### SCD Types

| Type | Meaning | Example |
|---|---|---|
| Type 0 | never change | original signup date |
| Type 1 | overwrite old value | corrected email |
| Type 2 | keep history with new row | customer address history |
| Type 3 | keep limited previous value in columns | current_region, previous_region |

### SCD Type 1

Overwrite existing record.

```sql
MERGE INTO dim_customer tgt
USING staging_customer src
  ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET
  tgt.email = src.email,
  tgt.city = src.city,
  tgt.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
  customer_id, email, city, created_at, updated_at
) VALUES (
  src.customer_id, src.email, src.city, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
);
```

Use Type 1 when history is not needed or the source value is a correction.

### SCD Type 2

Keep old row, expire it, insert new current row.

Typical columns:

- `customer_key` surrogate key
- `customer_id` natural key
- attributes such as `city`, `segment`
- `effective_start_date`
- `effective_end_date`
- `is_current`

Expire changed current records:

```sql
UPDATE dim_customer tgt
SET
  effective_end_date = CURRENT_DATE - INTERVAL '1 day',
  is_current = false
FROM staging_customer src
WHERE tgt.customer_id = src.customer_id
  AND tgt.is_current = true
  AND (
    tgt.city <> src.city
    OR tgt.segment <> src.segment
  );
```

Insert new rows:

```sql
INSERT INTO dim_customer (
  customer_id,
  email,
  city,
  segment,
  effective_start_date,
  effective_end_date,
  is_current
)
SELECT
  src.customer_id,
  src.email,
  src.city,
  src.segment,
  CURRENT_DATE,
  DATE '9999-12-31',
  true
FROM staging_customer src
LEFT JOIN dim_customer tgt
  ON src.customer_id = tgt.customer_id
 AND tgt.is_current = true
WHERE tgt.customer_id IS NULL
   OR tgt.city <> src.city
   OR tgt.segment <> src.segment;
```

In Databricks Delta, this is often implemented with `MERGE` plus staged rows.

### SCD Type 3

Keep current and previous value in the same row.

```sql
UPDATE dim_customer
SET previous_city = city,
    city = src.city
FROM staging_customer src
WHERE dim_customer.customer_id = src.customer_id
  AND dim_customer.city <> src.city;
```

**Key Takeaways**

- Type 1 overwrites.
- Type 2 preserves history.
- Type 2 needs effective dates and current flag.

**Common Mistakes**

- Updating Type 2 row instead of expiring and inserting.
- Not handling `NULL` comparisons.
- Creating overlapping effective date ranges.

**Interview Focus**

- Implement SCD Type 1 and Type 2.
- Explain surrogate keys.
- Join fact rows to the correct dimension version.

## 15. Transactions

A transaction is a unit of work that should succeed or fail together.

```sql
BEGIN;

UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;

COMMIT;
```

If something fails:

```sql
ROLLBACK;
```

### ACID

| Property | Practical meaning |
|---|---|
| Atomicity | all or nothing |
| Consistency | rules stay valid |
| Isolation | concurrent work does not corrupt results |
| Durability | committed data survives failure |

### Isolation Problems

| Problem | Meaning |
|---|---|
| Dirty read | read uncommitted data |
| Non-repeatable read | same row changes between reads |
| Phantom read | new matching rows appear between reads |

### Isolation Levels

| Level | Practical note |
|---|---|
| Read uncommitted | fastest, unsafe |
| Read committed | common default |
| Repeatable read | stronger consistency |
| Serializable | strongest, more blocking |

### Locks and Deadlocks

Locks protect data during writes. Deadlocks happen when transactions wait on each other in a cycle.

For data engineers, the practical lessons are:

- Keep transactions short.
- Process batches consistently.
- Avoid updating the same table in different key orders from different jobs.
- Understand whether your warehouse/lakehouse supports ACID transactions.

Delta Lake provides ACID transactions on data lake storage through its transaction log.

**Key Takeaways**

- Transactions matter for correctness during writes.
- Isolation is about concurrency behavior.
- Lakehouse formats like Delta bring transactional behavior to object storage.

**Common Mistakes**

- Long transactions around huge transformations.
- Assuming every storage layer supports rollback.

**Interview Focus**

- ACID.
- Isolation anomalies.
- Why Delta Lake transaction log matters.

## 16. Advanced SQL Analytics

### Top N

```sql
SELECT *
FROM products
ORDER BY revenue DESC
LIMIT 10;
```

### Top N per Group

```sql
WITH ranked AS (
  SELECT
    category,
    product_id,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM product_revenue
)
SELECT *
FROM ranked
WHERE rn <= 3;
```

### Gaps and Islands

Find consecutive login days.

```sql
WITH numbered AS (
  SELECT
    user_id,
    login_date,
    login_date - ROW_NUMBER() OVER (
      PARTITION BY user_id ORDER BY login_date
    ) * INTERVAL '1 day' AS island_key
  FROM user_logins
),
grouped AS (
  SELECT user_id, island_key, MIN(login_date) AS start_date, MAX(login_date) AS end_date, COUNT(*) AS days
  FROM numbered
  GROUP BY user_id, island_key
)
SELECT *
FROM grouped
WHERE days >= 3;
```

### Sessionization

```sql
WITH events_with_gap AS (
  SELECT
    user_id,
    event_ts,
    CASE
      WHEN LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL THEN 1
      WHEN event_ts > LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) + INTERVAL '30 minutes' THEN 1
      ELSE 0
    END AS new_session_flag
  FROM events
),
sessions AS (
  SELECT
    user_id,
    event_ts,
    SUM(new_session_flag) OVER (
      PARTITION BY user_id ORDER BY event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_number
  FROM events_with_gap
)
SELECT *
FROM sessions;
```

### Retention

```sql
WITH first_seen AS (
  SELECT user_id, MIN(event_date) AS cohort_date
  FROM events
  GROUP BY user_id
),
activity AS (
  SELECT DISTINCT user_id, event_date
  FROM events
)
SELECT
  f.cohort_date,
  a.event_date - f.cohort_date AS days_since_signup,
  COUNT(DISTINCT a.user_id) AS retained_users
FROM first_seen f
JOIN activity a
  ON f.user_id = a.user_id
GROUP BY f.cohort_date, a.event_date - f.cohort_date;
```

### Cohort Analysis

Group users by signup month and measure activity by later months.

```sql
WITH cohorts AS (
  SELECT user_id, DATE_TRUNC('month', MIN(event_date)) AS cohort_month
  FROM events
  GROUP BY user_id
),
activity AS (
  SELECT DISTINCT user_id, DATE_TRUNC('month', event_date) AS activity_month
  FROM events
)
SELECT
  c.cohort_month,
  a.activity_month,
  COUNT(DISTINCT a.user_id) AS active_users
FROM cohorts c
JOIN activity a ON c.user_id = a.user_id
GROUP BY c.cohort_month, a.activity_month;
```

### Funnel Analysis

```sql
SELECT
  COUNT(DISTINCT CASE WHEN event_name = 'view_product' THEN user_id END) AS viewed,
  COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_id END) AS added,
  COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) AS purchased
FROM events
WHERE event_date = CURRENT_DATE;
```

### Percentiles and Median

```sql
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY order_amount) AS median_order_amount
FROM orders;
```

Databricks commonly uses:

```sql
SELECT percentile_approx(order_amount, 0.5) AS median_order_amount
FROM orders;
```

### YoY / MoM / WoW

```sql
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month_start, SUM(amount) AS revenue
  FROM sales
  GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
  month_start,
  revenue,
  LAG(revenue, 1) OVER (ORDER BY month_start) AS previous_month,
  LAG(revenue, 12) OVER (ORDER BY month_start) AS same_month_last_year
FROM monthly;
```

**Key Takeaways**

- Advanced analytics is mostly joins, aggregation, dates, and windows combined well.
- Always define the grain.
- For time-series analysis, build a calendar table to avoid missing dates.

**Common Mistakes**

- Counting events instead of users in retention.
- Missing users with zero activity because no date spine exists.
- Incorrect session gaps due to timezone or unsorted events.

**Interview Focus**

- Gaps and islands.
- Sessionization.
- Retention/cohort/funnel.

## 17. Data Engineering SQL Patterns ⭐⭐⭐

### Full Load

Replace target with a complete refreshed dataset.

```sql
TRUNCATE TABLE dim_product;

INSERT INTO dim_product
SELECT *
FROM staging_product;
```

Use when data is small or source sends full snapshots.

### Incremental Load

Load only changed/new data.

```sql
SELECT *
FROM source_orders
WHERE updated_at > (
  SELECT last_watermark
  FROM etl_watermarks
  WHERE pipeline_name = 'orders'
);
```

### CDC

Change Data Capture captures inserts, updates, and deletes from source.

Typical columns:

- operation: `I`, `U`, `D`
- commit timestamp
- primary key
- before/after values

```sql
MERGE INTO target_orders t
USING cdc_orders s
  ON t.order_id = s.order_id
WHEN MATCHED AND s.operation = 'D' THEN DELETE
WHEN MATCHED AND s.operation = 'U' THEN UPDATE SET
  t.status = s.status,
  t.amount = s.amount,
  t.updated_at = s.commit_ts
WHEN NOT MATCHED AND s.operation IN ('I', 'U') THEN INSERT (
  order_id, status, amount, updated_at
) VALUES (
  s.order_id, s.status, s.amount, s.commit_ts
);
```

### Upsert

Use `MERGE` or dialect-specific upsert when keys may already exist.

### Deduplication

```sql
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY business_key
      ORDER BY event_ts DESC, ingestion_ts DESC
    ) AS rn
  FROM staging_table
)
SELECT *
FROM ranked
WHERE rn = 1;
```

### Data Reconciliation

```sql
SELECT
  'source' AS side,
  COUNT(*) AS row_count,
  SUM(amount) AS amount_sum
FROM source_orders
UNION ALL
SELECT
  'target',
  COUNT(*),
  SUM(amount)
FROM target_orders;
```

### Source-Target Validation

```sql
SELECT order_id, amount FROM source_orders
EXCEPT
SELECT order_id, amount FROM target_orders;
```

### Row-count Validation

```sql
SELECT
  s.source_count,
  t.target_count,
  s.source_count - t.target_count AS count_diff
FROM (SELECT COUNT(*) AS source_count FROM source_orders) s
CROSS JOIN (SELECT COUNT(*) AS target_count FROM target_orders) t;
```

### Duplicate Checks

```sql
SELECT order_id, COUNT(*) AS cnt
FROM target_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### NULL Checks

```sql
SELECT COUNT(*) AS missing_customer_id_count
FROM orders
WHERE customer_id IS NULL;
```

### Referential Integrity

```sql
SELECT o.*
FROM fact_orders o
LEFT JOIN dim_customer c
  ON o.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
```

### Late-arriving Data

Late-arriving data is data that arrives after the pipeline window it belongs to.

Pattern:

- process a lookback window, not only new timestamp
- merge into target to correct old partitions
- keep watermarks with safety delay

```sql
SELECT *
FROM source_events
WHERE event_date >= CURRENT_DATE - INTERVAL '3 days';
```

### Watermark-based Processing

```sql
WITH bounds AS (
  SELECT last_watermark
  FROM etl_watermarks
  WHERE pipeline_name = 'events'
),
changes AS (
  SELECT *
  FROM source_events s
  CROSS JOIN bounds b
  WHERE s.updated_at > b.last_watermark
    AND s.updated_at <= CURRENT_TIMESTAMP
)
SELECT *
FROM changes;
```

After successful load:

```sql
UPDATE etl_watermarks
SET last_watermark = (SELECT MAX(updated_at) FROM source_events)
WHERE pipeline_name = 'events';
```

**Key Takeaways**

- Incremental loading is about correctness first, speed second.
- CDC needs delete handling.
- Reconciliation queries are production tools, not interview-only ideas.

**Common Mistakes**

- Updating watermark before target load succeeds.
- Ignoring late-arriving data.
- Merging non-deduped source records.

**Interview Focus**

- Design incremental load.
- Write data quality checks.
- Explain CDC and watermarking.

## 18. SQL Performance Optimization ⭐⭐⭐

Performance is not magic. Most slow SQL comes from scanning too much data, shuffling too much data, joining at the wrong grain, or sorting too much data.

### Query Execution and EXPLAIN

Use `EXPLAIN` to see the plan.

```sql
EXPLAIN
SELECT customer_id, SUM(amount)
FROM orders
WHERE order_date >= DATE '2026-10-01'
GROUP BY customer_id;
```

Look for:

- full scans
- join type
- filters pushed down
- partition pruning
- sort/shuffle steps
- estimated rows vs actual rows

### Full Table Scans

A full scan is not always bad in columnar warehouses, but it is bad when avoidable.

Reduce scans by:

- filtering partition columns
- selecting only needed columns
- using clustering/sort keys/indexes
- maintaining statistics

### Indexes

Indexes speed up selective lookups in traditional databases.

```sql
CREATE INDEX idx_orders_customer_date
ON orders (customer_id, order_date);
```

### Composite Indexes

Order matters. `(customer_id, order_date)` helps:

```sql
WHERE customer_id = 10
  AND order_date >= DATE '2026-10-01'
```

It may not help as much for filtering only `order_date`.

### Covering Indexes

An index covers a query when all needed columns are available from the index.

Useful in OLTP databases; less relevant in columnar lakehouse systems.

### Partitioning and Partition Pruning

Partitioning physically organizes data by values like date.

```sql
WHERE event_date = DATE '2026-10-05'
```

This can prune partitions. This may not:

```sql
WHERE CAST(event_ts AS DATE) = DATE '2026-10-05'
```

### Predicate Pushdown

Push filters down to the storage/source layer so less data is read.

Columnar formats like Parquet support pushdown for many filters.

### Join Optimization

Good join habits:

- filter before joining
- project only needed columns
- join on properly typed keys
- avoid many-to-many explosions
- broadcast small dimensions in Spark

### Data Skew

Skew happens when a few keys have huge numbers of rows.

Symptoms:

- one Spark task runs much longer
- joins hang near the end
- shuffle partitions uneven

Fixes:

- broadcast small side
- salt skewed keys
- pre-aggregate
- handle hot keys separately
- use AQE skew join handling in Spark

### Spark / PySpark / Databricks Concepts

| Concept | Practical meaning |
|---|---|
| Shuffle | data moved across executors; expensive |
| Broadcast join | copy small table to all workers |
| Partitioning | split data for parallel processing |
| AQE | Adaptive Query Execution adjusts plan at runtime |
| Predicate pushdown | filter at file/source read |
| Partition pruning | skip irrelevant partitions |

Broadcast example:

```sql
SELECT /*+ BROADCAST(c) */
  o.order_id,
  c.customer_segment
FROM orders o
JOIN customers c
  ON o.customer_id = c.customer_id;
```

### Redshift Notes

Redshift performance depends heavily on:

- distribution style/key
- sort key
- compression encoding
- vacuum/analyze
- avoiding data redistribution for joins

### Optimization Checklist

1. Are you reading only needed columns?
2. Are filters applied early?
3. Are partition filters usable?
4. Are join keys unique where expected?
5. Is one side small enough to broadcast?
6. Is data skewed?
7. Are stats fresh?
8. Did `EXPLAIN` confirm your assumption?

**Key Takeaways**

- Reduce data before joins.
- Avoid accidental many-to-many joins.
- In Spark, shuffles are the expensive part.

**Common Mistakes**

- Optimizing syntax instead of data volume.
- Casting join keys during joins.
- Using `SELECT *` in large transformations.

**Interview Focus**

- Explain query plan basics.
- Spark shuffle and broadcast join.
- Partition pruning and predicate pushdown.

## 19. Data Warehouse / Lakehouse SQL

### Traditional Database vs Warehouse vs Lakehouse

| System | Main use | Storage style |
|---|---|---|
| Traditional database | app transactions | row-oriented |
| Data warehouse | analytics | columnar / managed |
| Lakehouse | analytics + data lake flexibility | open files + table format |

### Tables and Views

```sql
CREATE TABLE sales_summary AS
SELECT order_date, SUM(amount) AS revenue
FROM sales
GROUP BY order_date;
```

Views store logic, not data:

```sql
CREATE VIEW vw_daily_revenue AS
SELECT order_date, SUM(amount) AS revenue
FROM sales
GROUP BY order_date;
```

Materialized views store precomputed results where supported.

### Temporary Views

Common in Spark/Databricks sessions:

```sql
CREATE OR REPLACE TEMP VIEW recent_orders AS
SELECT *
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '7 days';
```

### CTAS

Create table as select:

```sql
CREATE TABLE curated_orders AS
SELECT *
FROM staging_orders
WHERE is_valid = true;
```

### External Tables and External Locations

External tables point to files outside warehouse-managed storage.

Databricks Unity Catalog uses external locations to manage governed access to cloud paths.

### Partitioning and Clustering

Partition by low/medium-cardinality columns like date. Cluster/Z-order by frequently filtered columns.

### Delta Tables

Delta Lake adds ACID transactions, schema evolution controls, time travel, merge, and change data feed on data lake files.

```sql
CREATE TABLE orders_delta
USING DELTA
PARTITIONED BY (order_date)
AS SELECT * FROM staging_orders;
```

### Time Travel

```sql
SELECT *
FROM orders_delta VERSION AS OF 10;
```

or timestamp-based syntax depending on platform.

### Change Data Feed

CDF lets downstream pipelines read row-level changes from Delta tables when enabled.

### OPTIMIZE and ZORDER

Databricks:

```sql
OPTIMIZE orders_delta
ZORDER BY (customer_id);
```

`OPTIMIZE` compacts files. `ZORDER` colocates related data for skipping.

### AWS Redshift

Important Redshift ideas:

- columnar storage
- distribution style and keys
- sort keys
- materialized views
- Spectrum external tables over S3
- COPY from S3

### Databricks SQL

Important Databricks SQL ideas:

- Delta tables
- Unity Catalog
- SQL warehouses
- temporary views
- MERGE
- OPTIMIZE / ZORDER
- Change Data Feed

**Key Takeaways**

- Warehouse SQL is optimized for analytics.
- Lakehouse SQL adds table formats over object storage.
- Databricks SQL plus Delta Lake is very merge/incremental friendly.

**Common Mistakes**

- Over-partitioning by high-cardinality columns.
- Creating too many tiny files.
- Treating object storage like a transactional database without a table format.

**Interview Focus**

- Redshift sort/dist keys.
- Delta Lake features.
- Warehouse vs lakehouse.

## 20. Semi-Structured Data

Modern pipelines often ingest JSON from APIs, Kafka, logs, and event streams.

### JSON

PostgreSQL:

```sql
SELECT payload ->> 'event_name' AS event_name
FROM raw_events;
```

Databricks:

```sql
SELECT get_json_object(payload, '$.event_name') AS event_name
FROM raw_events;
```

### Arrays

Databricks:

```sql
SELECT user_id, explode(product_ids) AS product_id
FROM user_cart_events;
```

### Structs / Nested Data

```sql
SELECT
  event_id,
  user.id AS user_id,
  user.country AS country
FROM events;
```

### Flattening Nested Data

```sql
SELECT
  e.event_id,
  item.product_id,
  item.quantity
FROM events e
LATERAL VIEW explode(e.items) exploded AS item;
```

PostgreSQL uses `jsonb_array_elements`; BigQuery uses `UNNEST`; Databricks uses `explode`.

**Key Takeaways**

- Semi-structured data usually becomes structured in silver/curated layers.
- Exploding arrays changes grain.
- Always track original event id when flattening.

**Common Mistakes**

- Exploding multiple arrays and causing row multiplication.
- Not validating missing JSON fields.

**Interview Focus**

- Parse JSON fields.
- Explode arrays.
- Explain grain after flattening.

## 21. SQL Security

Security is about giving people and jobs only the access they need.

### Users, Roles, Grants

```sql
CREATE ROLE analyst_role;

GRANT SELECT ON TABLE sales_summary TO analyst_role;

REVOKE SELECT ON TABLE raw_payments FROM analyst_role;
```

### Row-Level Security

Limit rows a user can see.

Example: regional managers only see their region.

### Column-Level Security

Hide sensitive columns like SSN, card number, salary.

### Data Masking

Show partial or transformed sensitive values.

```sql
SELECT
  customer_id,
  CONCAT('****', RIGHT(card_number, 4)) AS masked_card
FROM payments;
```

### Views for Security

```sql
CREATE VIEW analytics.safe_customers AS
SELECT customer_id, city, customer_segment
FROM raw.customers;
```

### Least Privilege

Give the minimum access required for the job. Production pipelines should use service principals or job roles, not personal users.

### Databricks Unity Catalog

Unity Catalog provides centralized governance for catalogs, schemas, tables, views, functions, external locations, lineage, and permissions.

**Key Takeaways**

- Use roles, not direct user grants when possible.
- Use views and masking for sensitive data.
- Unity Catalog is central to Databricks governance.

**Common Mistakes**

- Giving broad access to raw zones.
- Using personal credentials in pipelines.
- Exposing PII in marts.

**Interview Focus**

- Least privilege.
- Row/column-level security.
- Unity Catalog basics.

# SQL Interview Preparation for Senior Data Engineer

The answers below are intentionally interview-ready: short first, then deeper context.

## Basic Interview Questions

| # | Question | Short Answer | Detail / Example |
|---:|---|---|---|
| 1 | What is SQL? | A language to query and manipulate relational data. | Data engineers use it for transformations, validation, modeling, and analysis. |
| 2 | What is `SELECT`? | It chooses columns or expressions. | `SELECT order_id, amount FROM orders;` |
| 3 | What is `WHERE`? | It filters rows before aggregation. | Use it to reduce data early. |
| 4 | What is `DISTINCT`? | It removes duplicate result rows. | Applies to all selected columns. |
| 5 | What is `ORDER BY`? | It sorts query output. | Required for deterministic `LIMIT`. |
| 6 | What is `LIMIT`? | It restricts returned rows. | Syntax differs in SQL Server and Oracle. |
| 7 | What is an alias? | A temporary name for a table or column. | `orders o`, `amount AS revenue`. |
| 8 | What is `NULL`? | Unknown or missing value. | Use `IS NULL`, not `= NULL`. |
| 9 | What is `COALESCE`? | First non-null value. | `COALESCE(discount, 0)`. |
| 10 | What is `CAST`? | Type conversion. | Raw strings often need casting in staging. |
| 11 | What is `CASE`? | Conditional expression. | Used for business rules and flags. |
| 12 | What is `GROUP BY`? | Groups rows for aggregation. | Output grain changes to grouped columns. |
| 13 | What is `HAVING`? | Filters groups after aggregation. | `HAVING COUNT(*) > 1`. |
| 14 | `COUNT(*)` vs `COUNT(col)`? | `COUNT(*)` counts rows; `COUNT(col)` ignores nulls. | Common data quality interview question. |
| 15 | What is `IN`? | Checks if value is in a list/subquery. | Watch nulls with `NOT IN`. |
| 16 | What is `BETWEEN`? | Inclusive range filter. | Avoid for end-of-day timestamp ranges. |
| 17 | What is `LIKE`? | Pattern matching. | `%` any characters, `_` one character. |
| 18 | What is DDL? | Commands defining objects. | `CREATE`, `ALTER`, `DROP`. |
| 19 | What is DML? | Commands changing data. | `INSERT`, `UPDATE`, `DELETE`, `MERGE`. |
| 20 | What is a primary key? | Unique row identifier. | In warehouses it may be logical rather than enforced. |

## Intermediate Interview Questions

| # | Question | Short Answer | Detail / SQL Example |
|---:|---|---|---|
| 1 | Explain inner join. | Keeps matching rows only. | `a JOIN b ON a.id=b.id`. |
| 2 | Explain left join. | Keeps all left rows. | Use for enrichment and unmatched checks. |
| 3 | Explain full outer join. | Keeps all rows from both sides. | Useful in reconciliation. |
| 4 | Why do joins create duplicates? | Join key is non-unique on one or both sides. | Check with `GROUP BY key HAVING COUNT(*)>1`. |
| 5 | How do you find unmatched records? | Left join and filter right key null. | `WHERE dim.key IS NULL`. |
| 6 | What is a CTE? | Named query block. | Improves ETL readability. |
| 7 | CTE vs subquery? | CTE is named and easier to read. | Optimizer may treat similarly. |
| 8 | What is a window function? | Calculates across related rows without collapsing them. | `ROW_NUMBER() OVER (...)`. |
| 9 | `ROW_NUMBER` vs `RANK`? | `ROW_NUMBER` unique sequence; `RANK` keeps ties with gaps. | Use `DENSE_RANK` for ties without gaps. |
| 10 | How find latest row per customer? | `ROW_NUMBER` partitioned by customer ordered by timestamp desc. | Keep `rn=1`. |
| 11 | How remove duplicates? | Rank by business key and keep winner. | Needs deterministic ordering. |
| 12 | What is conditional aggregation? | Aggregate only rows meeting condition. | `SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END)`. |
| 13 | `UNION` vs `UNION ALL`? | `UNION` dedupes; `UNION ALL` keeps all rows. | Prefer `UNION ALL` in pipelines unless dedupe needed. |
| 14 | `EXCEPT` use case? | Find rows in source not target. | Validation/reconciliation. |
| 15 | What is a transaction? | Unit of work that commits or rolls back together. | Important for multi-step writes. |
| 16 | What is ACID? | Atomicity, Consistency, Isolation, Durability. | Correctness guarantees. |
| 17 | What is normalization? | Reduce duplication by splitting entities. | OLTP style. |
| 18 | What is denormalization? | Combine data for faster analytics. | OLAP style. |
| 19 | Fact vs dimension? | Fact is event/measure; dimension is context. | Sales fact, customer dimension. |
| 20 | Star vs snowflake? | Star has denormalized dimensions; snowflake normalizes them. | Star is common in analytics. |
| 21 | Natural vs surrogate key? | Source/business key vs generated warehouse key. | SCD Type 2 uses surrogate keys. |
| 22 | What is SCD Type 1? | Overwrite old value. | No history. |
| 23 | What is SCD Type 2? | Keep historical versions. | Effective dates and current flag. |
| 24 | What is MERGE? | Upsert statement. | Updates matched, inserts unmatched. |
| 25 | How handle nulls in joins? | `NULL` does not match `NULL`. | Use null-safe equality only when business needs it. |
| 26 | How parse JSON? | Use dialect JSON functions. | Databricks `get_json_object`, PostgreSQL `->>`. |
| 27 | What is exploding an array? | Convert array elements into rows. | Changes grain. |
| 28 | What is a temp table? | Session-scoped physical/intermediate table. | Useful when reused. |
| 29 | What is a view? | Saved query logic. | Does not store data unless materialized. |
| 30 | What is CTAS? | Create table as select. | Common in warehouse/lakehouse ELT. |

## Advanced Interview Questions

| # | Question | Short Answer | Detail / SQL Example |
|---:|---|---|---|
| 1 | How solve gaps and islands? | Create stable group key using date minus row number. | Useful for consecutive activity. |
| 2 | How sessionize events? | Use `LAG` to detect gaps, cumulative sum sessions. | 30-minute inactivity rule is common. |
| 3 | How calculate retention? | Cohort users by first date, join activity by later periods. | Count distinct users per cohort age. |
| 4 | How build funnel metrics? | Conditional distinct counts by event step. | Watch event ordering if funnel is strict. |
| 5 | How compute median? | Use percentile function. | `percentile_cont` or `percentile_approx`. |
| 6 | How calculate MoM growth? | Aggregate monthly, use `LAG`. | Divide by `NULLIF(previous,0)`. |
| 7 | How handle late data? | Reprocess lookback window and merge. | Do not rely only on exact last watermark. |
| 8 | How avoid duplicate MERGE matches? | Deduplicate source by target key before merge. | `ROW_NUMBER` staging. |
| 9 | What is idempotency? | Same run can be repeated safely. | Merge by key, deterministic transformations. |
| 10 | How validate reconciliation? | Compare counts, sums, hashes, and exceptions. | `EXCEPT`, grouped totals. |
| 11 | How design fact grain? | Define exactly one row means what. | Order item, order, transaction, event. |
| 12 | How join facts to SCD2 dimensions? | Join natural key and fact date between effective dates. | Use current flag only for current-state reporting. |
| 13 | How detect many-to-many join? | Count rows per join key on both sides. | Non-unique both sides is a warning. |
| 14 | Why use calendar table? | Fill missing dates and support time attributes. | Needed for zero-activity reporting. |
| 15 | How find second highest value? | `DENSE_RANK` ordered desc and filter rank 2. | Handles ties. |
| 16 | How find top N per group? | Rank partitioned by group. | `ROW_NUMBER` or `DENSE_RANK`. |
| 17 | How detect changed rows? | Compare hashes or tracked columns. | Use null-safe comparisons. |
| 18 | What are effective dates? | Validity range for a dimension version. | SCD2. |
| 19 | How handle deletes in CDC? | Delete, soft-delete, or expire based on target design. | Do not ignore delete events. |
| 20 | How flatten nested JSON safely? | Extract IDs, explode arrays one at a time, preserve parent key. | Validate missing fields. |
| 21 | What is data skew? | Uneven key distribution causing slow tasks. | Common in Spark joins. |
| 22 | How fix skew? | Broadcast, salt keys, pre-aggregate, AQE. | Handle hot keys separately. |
| 23 | What is predicate pushdown? | Filters applied at storage/source read. | Reduces data scanned. |
| 24 | What is partition pruning? | Skip irrelevant partitions. | Requires usable partition filter. |
| 25 | Why avoid functions on join keys? | Blocks indexes/pushdown and adds CPU. | Normalize before joining. |
| 26 | How optimize large join? | Filter, project, dedupe, broadcast small side, align partitioning. | Check explain plan. |
| 27 | How test SQL transformation? | Row counts, key uniqueness, null checks, sample records, business totals. | Add DQ queries. |
| 28 | What is a materialized view? | Stored result of a query. | Speeds repeated analytics, needs refresh. |
| 29 | How handle schema evolution? | Controlled add columns, explicit casts, expectations. | Delta supports schema evolution with care. |
| 30 | What is exactly-once processing? | Each business event applied once despite retries. | Requires idempotency and stable keys. |

## Data Engineering SQL Questions

| # | Question | Short Answer | Detail / SQL Example |
|---:|---|---|---|
| 1 | Full vs incremental load? | Full reloads all data; incremental loads changes. | Incremental needs watermark/CDC. |
| 2 | What is a watermark? | Last successfully processed boundary. | Usually timestamp or monotonically increasing id. |
| 3 | How update watermark safely? | After target commit succeeds. | Prevent data loss. |
| 4 | What is CDC? | Capturing source inserts, updates, deletes. | Used for near-real-time sync. |
| 5 | How handle CDC delete? | Delete, soft-delete, or expire target row. | Depends on business requirements. |
| 6 | What is bronze/silver/gold? | Raw, cleaned, business-ready layers. | Common lakehouse pattern. |
| 7 | How validate source-target row counts? | Compare counts by batch/date. | Group by partition, not only total. |
| 8 | How check duplicate business keys? | `GROUP BY key HAVING COUNT(*)>1`. | Run in staging and target. |
| 9 | How check null required fields? | Count nulls for required columns. | Fail or quarantine records. |
| 10 | What is referential integrity check? | Fact keys must exist in dimensions. | Left anti join pattern. |
| 11 | How design staging table? | Preserve raw fields plus metadata. | Include ingestion timestamp, source file, batch id. |
| 12 | Why keep ingestion metadata? | Debugging, replay, lineage. | Critical in production incidents. |
| 13 | How dedupe events? | Business event id plus latest ingestion. | Or hash payload when no id. |
| 14 | How handle out-of-order events? | Use event time plus lookback windows. | Do not rely only on arrival order. |
| 15 | What is late-arriving dimension? | Fact arrives before dimension row. | Use unknown member or retry enrichment. |
| 16 | What is late-arriving fact? | Old event arrives after partition processed. | Reprocess lookback and merge. |
| 17 | What is data reconciliation? | Proving source and target agree. | Counts, sums, exception rows. |
| 18 | What is source-target validation? | Compare records/metrics between layers. | `EXCEPT`, checksums. |
| 19 | How build aggregate table? | Define grain and aggregate from reliable base fact. | Avoid aggregating aggregates unless valid. |
| 20 | What is an audit table? | Tracks pipeline runs and metrics. | start/end time, row counts, status. |
| 21 | What is an idempotent pipeline? | Safe to rerun without double counting. | Use merge/delete-insert by partition. |
| 22 | How load partition safely? | Replace only target partition. | Dynamic partition overwrite or delete+insert. |
| 23 | What is data quality rule? | SQL check that confirms expected condition. | not null, uniqueness, accepted values. |
| 24 | What is quarantine table? | Stores rejected records. | Keeps pipeline observable. |
| 25 | How handle type errors? | Try-cast, reject invalid rows, track counts. | Avoid silent nulling. |
| 26 | How compare hashes? | Hash selected normalized columns. | Good for change detection. |
| 27 | What is soft delete? | Mark row deleted without removing. | `is_deleted`, `deleted_at`. |
| 28 | How implement SCD1? | Merge and overwrite attributes. | Current state only. |
| 29 | How implement SCD2? | Expire current row, insert new version. | Effective dates. |
| 30 | How handle replay? | Use deterministic batch keys and idempotent writes. | Avoid appending duplicate output. |

## SQL Optimization Questions

| # | Question | Short Answer | Detail / Example |
|---:|---|---|---|
| 1 | First step for slow query? | Check execution plan and data volume. | Do not guess. |
| 2 | Why avoid `SELECT *`? | Reads unnecessary columns. | Bad for columnar scans and network. |
| 3 | What is full table scan? | Engine reads entire table. | Sometimes fine in warehouses, bad if avoidable. |
| 4 | What is index? | Structure speeding lookups. | More OLTP/traditional DB focused. |
| 5 | What is composite index? | Multi-column index. | Leading column matters. |
| 6 | What is partition pruning? | Skip partitions not matching filter. | Needs partition-column filter. |
| 7 | What is predicate pushdown? | Push filters to storage/source. | Reduces scanned data. |
| 8 | What is broadcast join? | Copy small table to workers. | Avoids large shuffle in Spark. |
| 9 | What is shuffle? | Data movement across workers. | Expensive in Spark. |
| 10 | How reduce shuffle? | Filter/project/pre-aggregate/broadcast. | Also partition wisely. |
| 11 | What is data skew? | Uneven key distribution. | One task gets too much data. |
| 12 | How detect skew? | Task times, key counts, Spark UI. | Count rows by join key. |
| 13 | How fix skew? | Salting, broadcast, AQE, split hot keys. | No single universal fix. |
| 14 | Why keep stats fresh? | Optimizer needs row estimates. | Redshift/warehouse plans improve. |
| 15 | Why avoid casting in predicates? | Can block index/pruning. | Cast literals instead of columns. |
| 16 | How optimize joins? | Reduce rows before join, join on full keys, broadcast small side. | Validate cardinality. |
| 17 | How optimize aggregations? | Pre-filter, group at correct grain, avoid distinct if possible. | Distinct is expensive. |
| 18 | What are tiny files? | Too many small data files. | Hurts Spark/Delta reads. |
| 19 | What does `OPTIMIZE` do in Delta? | Compacts files. | Often paired with ZORDER. |
| 20 | What is ZORDER? | Colocates related data for skipping. | Good for frequent filter columns. |

# How to Think When Solving SQL Problems

Do not start by typing SQL. Start by understanding the shape of the answer.

1. Understand the expected output.
   Ask: one row per what? customer, order, day, customer-month?

2. Identify required tables.
   Which table has the event? Which table has descriptive attributes?

3. Identify relationships.
   What are the join keys? Are they unique?

4. Decide whether JOIN is required.
   If you only need existence, use `EXISTS`. If you need attributes, use `JOIN`.

5. Filter data.
   Apply date/status/source filters early.

6. Aggregate if required.
   Check output grain before writing `GROUP BY`.

7. Use window functions if required.
   Latest row, ranking, previous/next, running totals.

8. Handle duplicates.
   Know whether duplicates are valid or accidental.

9. Validate NULL behavior.
   Joins, counts, comparisons, and `NOT IN` all change with nulls.

10. Validate the final result.
    Compare row counts, spot-check examples, and test edge cases.

### Example: Latest Successful Transaction per Customer

Expected output: one row per customer.  
Required table: `transactions`.  
Filters: `status = 'SUCCESS'`.  
Window needed: yes, latest per customer.

```sql
WITH successful AS (
  SELECT *
  FROM transactions
  WHERE status = 'SUCCESS'
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY transaction_ts DESC, transaction_id DESC
    ) AS rn
  FROM successful
)
SELECT customer_id, transaction_id, transaction_ts, amount
FROM ranked
WHERE rn = 1;
```

### Example: Customers with No Orders

Expected output: customers only.  
Required tables: `customers`, `orders`.  
Join required: yes, anti-join.

```sql
SELECT c.customer_id, c.customer_name
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

# Practice Section

To keep practice readable, the questions use these compact sample tables. Each question lists its required table structure, sample rows, and expected output shape. Do not jump to solutions first; try to write the SQL yourself.

## Level 1 — Basic

| # | Problem | Sample table structure | Sample data | Expected output |
|---:|---|---|---|---|
| 1 | Select all customers. | `customers(customer_id, name, city)` | `(1,Asha,Pune),(2,Ben,Delhi)` | both rows |
| 2 | Select customer names only. | `customers(customer_id, name, city)` | same | `Asha`,`Ben` |
| 3 | Find orders above 100. | `orders(order_id, amount)` | `(1,80),(2,150)` | order 2 |
| 4 | Find distinct cities. | `customers(customer_id, city)` | `(1,Pune),(2,Pune),(3,Delhi)` | Pune, Delhi |
| 5 | Sort products by price high to low. | `products(product_id, price)` | `(1,10),(2,30)` | product 2, product 1 |
| 6 | Return first 5 events. | `events(event_id, event_ts)` | rows 1-10 | 5 rows |
| 7 | Create full name. | `employees(first_name,last_name)` | `(Asha,Rao)` | `Asha Rao` |
| 8 | Flag high value orders. | `orders(order_id, amount)` | `(1,50),(2,500)` | low, high |
| 9 | Replace null discount with 0. | `orders(order_id, discount)` | `(1,NULL),(2,10)` | 0, 10 |
| 10 | Cast string amount. | `raw_orders(order_id, amount_text)` | `(1,'10.50')` | decimal 10.50 |
| 11 | Find customers from Pune. | `customers(customer_id, city)` | `(1,Pune),(2,Delhi)` | customer 1 |
| 12 | Find active users. | `users(user_id, is_active)` | `(1,true),(2,false)` | user 1 |
| 13 | Find products not in category X. | `products(product_id, category)` | `(1,X),(2,Y)` | product 2 |
| 14 | Get order dates only from timestamp. | `orders(order_id, order_ts)` | `(1,2026-10-05 10:00)` | `2026-10-05` |
| 15 | Normalize email to lowercase. | `customers(email)` | `A@EXAMPLE.COM` | `a@example.com` |
| 16 | Trim product codes. | `products(code)` | `' ABC '` | `ABC` |
| 17 | Find missing phone numbers. | `customers(customer_id, phone)` | `(1,NULL),(2,999)` | customer 1 |
| 18 | Find non-null emails. | `customers(customer_id,email)` | `(1,NULL),(2,a@x.com)` | customer 2 |
| 19 | Show current date. | no table | none | today's date |
| 20 | Alias amount as revenue. | `orders(amount)` | `(100)` | `revenue=100` |

## Level 2 — Intermediate

| # | Problem | Sample table structure | Sample data | Expected output |
|---:|---|---|---|---|
| 1 | Count orders per customer. | `orders(order_id,customer_id)` | `(1,10),(2,10),(3,20)` | `10=2,20=1` |
| 2 | Revenue per day. | `orders(order_date,amount)` | `(2026-10-01,10),(2026-10-01,20)` | `2026-10-01=30` |
| 3 | Customers with more than 2 orders. | `orders(order_id,customer_id)` | `10 has 3, 20 has 1` | customer 10 |
| 4 | Join orders to customers. | `orders(order_id,customer_id)`, `customers(customer_id,name)` | `(1,10),(10,Asha)` | order 1 Asha |
| 5 | Customers with no orders. | `customers`, `orders` | customers 10,20; order for 10 | customer 20 |
| 6 | Orders with invalid customer. | `orders`, `customers` | order customer 99 missing | order with 99 |
| 7 | Total sales by product category. | `sales(product_id,amount)`, `products(product_id,category)` | p1 phone 100, p2 tv 200 | category totals |
| 8 | Distinct buyers per product. | `orders(customer_id,product_id)` | repeated buyer rows | distinct counts |
| 9 | Average order value by city. | `orders`, `customers(city)` | simple rows | avg per city |
| 10 | Completed and cancelled order counts. | `orders(status)` | COMPLETE,CANCELLED | counts columns |
| 11 | Union web and store customers. | `web_customers`, `store_customers` | overlapping IDs | unique IDs |
| 12 | Append monthly order tables. | `orders_sep`, `orders_oct` | separate rows | all rows |
| 13 | Source rows missing in target. | `source_orders`, `target_orders` | source has extra id | extra id |
| 14 | Find common customers in CRM and billing. | `crm_customers`, `billing_customers` | overlap id 2 | id 2 |
| 15 | Extract email domain. | `customers(email)` | `a@gmail.com` | `gmail.com` |
| 16 | Orders in last 7 days. | `orders(order_date)` | recent/old rows | recent rows |
| 17 | Monthly revenue. | `orders(order_date,amount)` | Oct rows | Oct total |
| 18 | Find duplicate order IDs. | `orders(order_id)` | id 1 twice | id 1 |
| 19 | Deduplicate latest raw customer. | `raw_customers(customer_id,updated_at)` | two rows per id | latest |
| 20 | Top 3 products by revenue. | `sales(product_id,amount)` | many rows | 3 products |
| 21 | Rank employees by salary. | `employees(dept_id,salary)` | ties | ranks |
| 22 | Previous transaction per customer. | `transactions(customer_id,transaction_ts)` | ordered rows | previous ts |
| 23 | Running daily revenue. | `daily_sales(sale_date,revenue)` | day rows | cumulative sum |
| 24 | Moving 3-day average. | `daily_sales(sale_date,revenue)` | day rows | moving avg |
| 25 | Find customers who ordered product 5. | `orders(customer_id,product_id)` | product rows | matching customers |
| 26 | Find customers with any order. | `customers`, `orders` | some customers have orders | customers with orders |
| 27 | Count null emails by city. | `customers(city,email)` | null rows | null count city |
| 28 | Standardize country codes. | `customers(country)` | `india`, `IN` | `IN` |
| 29 | Build customer order summary. | `orders(customer_id,amount)` | simple rows | count,sum,max |
| 30 | Find orders between two dates. | `orders(order_date)` | dates | matching range |

## Level 3 — Advanced

| # | Problem | Sample table structure | Sample data | Expected output |
|---:|---|---|---|---|
| 1 | Latest order per customer. | `orders(customer_id,order_id,order_ts)` | multiple per customer | one latest each |
| 2 | Second highest salary. | `employees(employee_id,salary)` | salaries with ties | rank 2 salaries |
| 3 | Top 3 products per category. | `product_sales(category,product_id,revenue)` | many products | 3 per category |
| 4 | Month-over-month growth. | `orders(order_date,amount)` | monthly data | month, growth |
| 5 | User retention by cohort day. | `events(user_id,event_date)` | signup/activity | cohort retention |
| 6 | Sessionize clickstream. | `events(user_id,event_ts)` | gaps >30 min | session numbers |
| 7 | Consecutive login streaks. | `logins(user_id,login_date)` | date rows | streaks |
| 8 | Detect gaps in order IDs. | `orders(order_id)` | missing ids | gap ranges |
| 9 | Median order amount. | `orders(amount)` | amounts | median |
| 10 | 95th percentile latency. | `api_events(latency_ms)` | latencies | p95 |
| 11 | First purchase after signup. | `users`, `orders` | signup/order dates | first purchase |
| 12 | Customers active 3 months in row. | `events(user_id,event_month)` | monthly rows | users |
| 13 | Find churned users. | `events(user_id,event_date)` | last activity | inactive users |
| 14 | Strict funnel view-cart-buy. | `events(user_id,event_name,event_ts)` | event sequence | users completing |
| 15 | Remove duplicate events by event_id. | `raw_events(event_id,ingestion_ts)` | duplicate ids | latest per id |
| 16 | Match fact to SCD2 dimension. | `fact_orders`, `dim_customer_scd2` | effective ranges | correct version |
| 17 | Compare source and target checksums. | `source_orders`, `target_orders` | rows | mismatch keys |
| 18 | Find overlapping SCD2 records. | `dim_customer(customer_id,start,end)` | overlap ranges | bad keys |
| 19 | Reconstruct current state from CDC. | `cdc(customer_id,op,commit_ts)` | I/U/D stream | latest non-deleted |
| 20 | Identify late-arriving events. | `events(event_ts,ingestion_ts)` | delayed rows | late rows |
| 21 | Build date spine revenue report. | `calendar`, `orders` | missing sales days | zero-filled days |
| 22 | Rolling 7-day active users. | `events(user_id,event_date)` | activity rows | date, active users |
| 23 | Customers above category average. | `orders`, `products` | revenue rows | customers |
| 24 | Find many-to-many join risk. | `a(key)`, `b(key)` | duplicate keys both | risky keys |
| 25 | Pivot channel revenue. | `sales(customer_id,channel,amount)` | web/store rows | columns |
| 26 | Unpivot monthly columns. | `wide_sales(customer_id,jan,feb)` | wide rows | month rows |
| 27 | Parse nested JSON event. | `raw_events(payload)` | JSON strings | extracted fields |
| 28 | Explode order items array. | `orders(order_id,items)` | array items | one row per item |
| 29 | Find first and last event per user. | `events(user_id,event_ts)` | events | first,last |
| 30 | Calculate YoY revenue. | `sales(order_date,amount)` | two years | YoY growth |

## Level 4 — Data Engineering

| # | Problem | Sample table structure | Sample data | Expected output |
|---:|---|---|---|---|
| 1 | Incremental extract by watermark. | `source_orders(updated_at)`, `watermarks` | last watermark | changed rows |
| 2 | Upsert staging into target. | `stg_orders`, `fact_orders` | existing/new rows | merged target |
| 3 | Insert-only load. | `stg_events`, `fact_events` | duplicate event id | only new |
| 4 | CDC merge with deletes. | `cdc_orders(op)`, `orders` | I/U/D ops | target state |
| 5 | SCD Type 1 customer load. | `stg_customer`, `dim_customer` | changed city | overwritten row |
| 6 | SCD Type 2 customer load. | same | changed segment | expired + new row |
| 7 | Row count validation by batch. | `source`, `target` | batch ids | count diff |
| 8 | Amount reconciliation by date. | `source_orders`, `target_orders` | amounts | diff by date |
| 9 | Null quality check. | `orders(customer_id)` | null rows | failed count |
| 10 | Duplicate key quality check. | `orders(order_id)` | dupes | bad ids |
| 11 | Referential integrity check. | `fact_orders`, `dim_customer` | missing dim key | bad facts |
| 12 | Quarantine invalid records. | `raw_orders` | bad amount/null id | invalid rows |
| 13 | Build bronze to silver transform. | `raw_orders` | string fields | typed clean rows |
| 14 | Build silver to gold aggregate. | `silver_orders` | clean orders | daily revenue |
| 15 | Partition reload for one day. | `orders_target`, `orders_stage` | date partition | replaced day |
| 16 | Late-arriving data lookback. | `events` | old event newly ingested | lookback rows |
| 17 | Update watermark after success. | `watermarks`, `source` | max updated_at | new watermark |
| 18 | Audit pipeline metrics. | `audit`, `stage` | row counts | audit row |
| 19 | Detect schema drift columns. | `information_schema.columns` | source/target cols | missing cols |
| 20 | Compare hashes for change. | `src`, `tgt` | changed attr | changed keys |
| 21 | Soft delete missing source records. | `src_keys`, `target` | missing key | is_deleted true |
| 22 | Load unknown dimension member. | `dim_customer` | none | key -1 row |
| 23 | Facts with late dimension. | `orders`, `dim_customer` | missing customer | unknown key |
| 24 | Backfill monthly aggregate. | `orders` | old months | refreshed months |
| 25 | Idempotent daily append. | `stage_events`, `fact_events` | rerun batch | no duplicates |
| 26 | Validate accepted status values. | `orders(status)` | invalid status | bad statuses |
| 27 | File-level ingestion audit. | `raw(file_name,batch_id)` | files | counts per file |
| 28 | CDC latest event per key. | `cdc(key,commit_ts)` | multiple ops | latest op |
| 29 | Merge only changed columns. | `src`, `tgt` | same/changed rows | update changed |
| 30 | Source-target exception report. | `src`, `tgt` | mismatches | missing/different rows |

## Level 5 — Senior Data Engineer Interview

| # | Problem | Sample table structure | Sample data | Expected output |
|---:|---|---|---|---|
| 1 | Design SQL for daily revenue mart. | `orders`, `customers`, `products` | order items | date/category revenue |
| 2 | Build customer 360 summary. | `customers`, `orders`, `events` | multi-source | one row/customer |
| 3 | Detect revenue drop anomaly. | `daily_revenue` | dates/revenue | anomalous dates |
| 4 | Reconcile CDC target to source snapshot. | `source_snapshot`, `target` | differences | exception report |
| 5 | Implement SCD2 with null-safe changes. | `stg_customer`, `dim_customer` | null changes | correct versions |
| 6 | Join order fact to SCD2 customer. | `fact_orders`, `dim_customer` | effective dates | historical attrs |
| 7 | Session funnel conversion. | `events` | sessions/events | conversion by session |
| 8 | 7-day rolling conversion. | `events` | daily view/buy | rolling rate |
| 9 | Cohort retention monthly. | `users`, `events` | signup/activity | cohort matrix |
| 10 | Skewed join mitigation query. | `large_events`, `small_dim` | hot key | broadcast/filter SQL |
| 11 | Find duplicate facts caused by dim join. | `fact`, `dim` | duplicate dim keys | duplicate cause |
| 12 | Backfill with idempotent partition logic. | `stage`, `target` | date range | refreshed partitions |
| 13 | Compare exact record differences. | `src`, `tgt` | changed rows | change_type |
| 14 | Build DQ scorecard. | `orders` | null/dupe/bad status | metrics table |
| 15 | Handle out-of-order CDC. | `cdc` | older commits arrive late | latest valid state |
| 16 | Deduplicate with priority source. | `records(source_system,updated_at)` | CRM/API rows | winner by source priority |
| 17 | Revenue by fiscal month. | `sales`, `calendar` | fiscal mapping | fiscal revenue |
| 18 | Product hierarchy rollup. | `products(parent_id)`, `sales` | hierarchy | category revenue |
| 19 | User journey path. | `events(user_id,event_name,event_ts)` | ordered events | path string |
| 20 | Detect bot users. | `events` | high event rate | suspected bots |
| 21 | SLA breach query. | `pipeline_runs` | start/end/status | breached jobs |
| 22 | Watermark with safety delay. | `source`, `watermark` | recent updates | bounded changes |
| 23 | Gold table incremental aggregate. | `orders`, `daily_revenue` | updates | merged aggregate |
| 24 | Late fact correction. | `facts`, `stage` | changed old order | corrected fact |
| 25 | Build bridge for many-to-many. | `orders`, `promotions` | multiple promos | bridge rows |
| 26 | Calculate customer lifetime value. | `orders` | customer spend | LTV |
| 27 | Percentile latency by endpoint. | `api_events` | endpoints | p50/p95/p99 |
| 28 | Inventory snapshot from movements. | `inventory_events` | in/out events | daily stock |
| 29 | Detect missing daily partitions. | `expected_dates`, `loaded_partitions` | missing date | missing partitions |
| 30 | Explain and improve slow SQL. | any large query | plan symptoms | optimized approach |

## Solutions

The solutions below favor ANSI SQL. Adjust date and JSON functions for your SQL dialect.

### Level 1 Solutions

```sql
-- 1
SELECT * FROM customers;
-- 2
SELECT name FROM customers;
-- 3
SELECT * FROM orders WHERE amount > 100;
-- 4
SELECT DISTINCT city FROM customers;
-- 5
SELECT * FROM products ORDER BY price DESC;
-- 6
SELECT * FROM events ORDER BY event_ts LIMIT 5;
-- 7
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM employees;
-- 8
SELECT order_id, CASE WHEN amount >= 100 THEN 'high' ELSE 'low' END AS value_band FROM orders;
-- 9
SELECT order_id, COALESCE(discount, 0) AS discount FROM orders;
-- 10
SELECT order_id, CAST(amount_text AS DECIMAL(18,2)) AS amount FROM raw_orders;
-- 11
SELECT * FROM customers WHERE city = 'Pune';
-- 12
SELECT * FROM users WHERE is_active = true;
-- 13
SELECT * FROM products WHERE category <> 'X';
-- 14
SELECT order_id, CAST(order_ts AS DATE) AS order_date FROM orders;
-- 15
SELECT LOWER(email) AS email FROM customers;
-- 16
SELECT TRIM(code) AS code FROM products;
-- 17
SELECT * FROM customers WHERE phone IS NULL;
-- 18
SELECT * FROM customers WHERE email IS NOT NULL;
-- 19
SELECT CURRENT_DATE;
-- 20
SELECT amount AS revenue FROM orders;
```

### Level 2 Solutions

```sql
-- 1
SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id;
-- 2
SELECT order_date, SUM(amount) AS revenue FROM orders GROUP BY order_date;
-- 3
SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(*) > 2;
-- 4
SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
-- 5
SELECT c.* FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.customer_id IS NULL;
-- 6
SELECT o.* FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL;
-- 7
SELECT p.category, SUM(s.amount) AS revenue FROM sales s JOIN products p ON s.product_id = p.product_id GROUP BY p.category;
-- 8
SELECT product_id, COUNT(DISTINCT customer_id) AS buyers FROM orders GROUP BY product_id;
-- 9
SELECT c.city, AVG(o.amount) AS avg_order_value FROM orders o JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.city;
-- 10
SELECT SUM(CASE WHEN status='COMPLETE' THEN 1 ELSE 0 END) AS completed, SUM(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) AS cancelled FROM orders;
-- 11
SELECT customer_id FROM web_customers UNION SELECT customer_id FROM store_customers;
-- 12
SELECT * FROM orders_sep UNION ALL SELECT * FROM orders_oct;
-- 13
SELECT order_id FROM source_orders EXCEPT SELECT order_id FROM target_orders;
-- 14
SELECT customer_id FROM crm_customers INTERSECT SELECT customer_id FROM billing_customers;
-- 15 Databricks/PostgreSQL style varies
SELECT SPLIT(email, '@')[1] AS domain FROM customers;
-- 16
SELECT * FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '7 days';
-- 17
SELECT DATE_TRUNC('month', order_date) AS month_start, SUM(amount) AS revenue FROM orders GROUP BY DATE_TRUNC('month', order_date);
-- 18
SELECT order_id, COUNT(*) AS cnt FROM orders GROUP BY order_id HAVING COUNT(*) > 1;
-- 19
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) rn FROM raw_customers) SELECT * FROM r WHERE rn=1;
-- 20
SELECT product_id, SUM(amount) AS revenue FROM sales GROUP BY product_id ORDER BY revenue DESC LIMIT 3;
-- 21
SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS salary_rank FROM employees;
-- 22
SELECT *, LAG(transaction_ts) OVER (PARTITION BY customer_id ORDER BY transaction_ts) AS previous_transaction_ts FROM transactions;
-- 23
SELECT sale_date, revenue, SUM(revenue) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue FROM daily_sales;
-- 24
SELECT sale_date, revenue, AVG(revenue) OVER (ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3d FROM daily_sales;
-- 25
SELECT DISTINCT customer_id FROM orders WHERE product_id = 5;
-- 26
SELECT c.* FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);
-- 27
SELECT city, SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email_count FROM customers GROUP BY city;
-- 28
SELECT customer_id, CASE WHEN UPPER(country) IN ('INDIA','IN') THEN 'IN' ELSE UPPER(country) END AS country_code FROM customers;
-- 29
SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS revenue, MAX(amount) AS max_order_amount FROM orders GROUP BY customer_id;
-- 30
SELECT * FROM orders WHERE order_date >= DATE '2026-10-01' AND order_date < DATE '2026-11-01';
```

### Level 3 Solutions

```sql
-- 1
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC, order_id DESC) rn FROM orders) SELECT * FROM r WHERE rn=1;
-- 2
WITH r AS (SELECT *, DENSE_RANK() OVER (ORDER BY salary DESC) rnk FROM employees) SELECT * FROM r WHERE rnk=2;
-- 3
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) rn FROM product_sales) SELECT * FROM r WHERE rn<=3;
-- 4
WITH m AS (SELECT DATE_TRUNC('month', order_date) month_start, SUM(amount) revenue FROM orders GROUP BY DATE_TRUNC('month', order_date))
SELECT month_start, revenue, (revenue - LAG(revenue) OVER (ORDER BY month_start)) / NULLIF(LAG(revenue) OVER (ORDER BY month_start),0) AS mom_growth FROM m;
-- 5
WITH c AS (SELECT user_id, MIN(event_date) cohort_date FROM events GROUP BY user_id), a AS (SELECT DISTINCT user_id, event_date FROM events)
SELECT c.cohort_date, a.event_date - c.cohort_date AS day_number, COUNT(DISTINCT a.user_id) users FROM c JOIN a ON c.user_id=a.user_id GROUP BY c.cohort_date, a.event_date - c.cohort_date;
-- 6
WITH g AS (SELECT *, CASE WHEN LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) IS NULL OR event_ts > LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) + INTERVAL '30 minutes' THEN 1 ELSE 0 END new_session FROM events)
SELECT *, SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_ts) AS session_id FROM g;
-- 7
WITH n AS (SELECT user_id, login_date, login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) * INTERVAL '1 day' grp FROM logins)
SELECT user_id, MIN(login_date) start_date, MAX(login_date) end_date, COUNT(*) days FROM n GROUP BY user_id, grp;
-- 8
WITH n AS (SELECT order_id, LEAD(order_id) OVER (ORDER BY order_id) next_id FROM orders) SELECT order_id + 1 AS gap_start, next_id - 1 AS gap_end FROM n WHERE next_id > order_id + 1;
-- 9
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount FROM orders;
-- 10
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency FROM api_events;
-- 11
SELECT u.user_id, MIN(o.order_date) AS first_purchase_date FROM users u JOIN orders o ON u.user_id=o.user_id AND o.order_date>=u.signup_date GROUP BY u.user_id;
-- 12
WITH n AS (SELECT DISTINCT user_id, event_month, event_month - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_month) * INTERVAL '1 month' grp FROM events)
SELECT user_id FROM n GROUP BY user_id, grp HAVING COUNT(*) >= 3;
-- 13
SELECT user_id FROM events GROUP BY user_id HAVING MAX(event_date) < CURRENT_DATE - INTERVAL '30 days';
-- 14
SELECT user_id FROM events GROUP BY user_id HAVING MIN(CASE WHEN event_name='view' THEN event_ts END) < MIN(CASE WHEN event_name='cart' THEN event_ts END) AND MIN(CASE WHEN event_name='cart' THEN event_ts END) < MIN(CASE WHEN event_name='buy' THEN event_ts END);
-- 15
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_ts DESC) rn FROM raw_events) SELECT * FROM r WHERE rn=1;
-- 16
SELECT f.*, d.customer_key FROM fact_orders f JOIN dim_customer_scd2 d ON f.customer_id=d.customer_id AND f.order_date BETWEEN d.effective_start_date AND d.effective_end_date;
-- 17
SELECT order_id FROM source_orders EXCEPT SELECT order_id FROM target_orders UNION ALL SELECT order_id FROM target_orders EXCEPT SELECT order_id FROM source_orders;
-- 18
SELECT a.customer_id FROM dim_customer a JOIN dim_customer b ON a.customer_id=b.customer_id AND a.customer_key<>b.customer_key AND a.effective_start_date <= b.effective_end_date AND b.effective_start_date <= a.effective_end_date;
-- 19
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY commit_ts DESC) rn FROM cdc) SELECT * FROM r WHERE rn=1 AND op <> 'D';
-- 20
SELECT * FROM events WHERE ingestion_ts > event_ts + INTERVAL '1 day';
-- 21
SELECT c.calendar_date, COALESCE(SUM(o.amount),0) revenue FROM calendar c LEFT JOIN orders o ON c.calendar_date=o.order_date GROUP BY c.calendar_date;
-- 22
SELECT d.calendar_date, COUNT(DISTINCT e.user_id) active_users FROM calendar d LEFT JOIN events e ON e.event_date BETWEEN d.calendar_date - INTERVAL '6 days' AND d.calendar_date GROUP BY d.calendar_date;
-- 23
WITH cr AS (SELECT o.customer_id,p.category,SUM(o.amount) revenue FROM orders o JOIN products p ON o.product_id=p.product_id GROUP BY o.customer_id,p.category), ar AS (SELECT category,AVG(revenue) avg_revenue FROM cr GROUP BY category) SELECT cr.* FROM cr JOIN ar ON cr.category=ar.category WHERE cr.revenue>ar.avg_revenue;
-- 24
SELECT a.key FROM (SELECT key,COUNT(*) ca FROM a GROUP BY key) a JOIN (SELECT key,COUNT(*) cb FROM b GROUP BY key) b ON a.key=b.key WHERE ca>1 AND cb>1;
-- 25
SELECT customer_id, SUM(CASE WHEN channel='web' THEN amount ELSE 0 END) web_amount, SUM(CASE WHEN channel='store' THEN amount ELSE 0 END) store_amount FROM sales GROUP BY customer_id;
-- 26
SELECT customer_id, 'jan' AS month_name, jan AS amount FROM wide_sales UNION ALL SELECT customer_id, 'feb', feb FROM wide_sales;
-- 27 Databricks
SELECT get_json_object(payload,'$.event_name') AS event_name FROM raw_events;
-- 28 Databricks
SELECT order_id, explode(items) AS item FROM orders;
-- 29
SELECT user_id, MIN(event_ts) first_event_ts, MAX(event_ts) last_event_ts FROM events GROUP BY user_id;
-- 30
WITH y AS (SELECT EXTRACT(YEAR FROM order_date) yr, SUM(amount) revenue FROM sales GROUP BY EXTRACT(YEAR FROM order_date)) SELECT yr,revenue,LAG(revenue) OVER (ORDER BY yr) prev_year_revenue FROM y;
```

### Level 4 Solutions

```sql
-- 1
SELECT s.* FROM source_orders s JOIN watermarks w ON w.pipeline_name='orders' WHERE s.updated_at > w.last_watermark;
-- 2
MERGE INTO fact_orders t USING stg_orders s ON t.order_id=s.order_id WHEN MATCHED THEN UPDATE SET amount=s.amount,status=s.status WHEN NOT MATCHED THEN INSERT (order_id,amount,status) VALUES (s.order_id,s.amount,s.status);
-- 3
INSERT INTO fact_events SELECT s.* FROM stg_events s WHERE NOT EXISTS (SELECT 1 FROM fact_events f WHERE f.event_id=s.event_id);
-- 4
MERGE INTO orders t USING cdc_orders s ON t.order_id=s.order_id WHEN MATCHED AND s.op='D' THEN DELETE WHEN MATCHED THEN UPDATE SET status=s.status,amount=s.amount WHEN NOT MATCHED AND s.op IN ('I','U') THEN INSERT (order_id,status,amount) VALUES (s.order_id,s.status,s.amount);
-- 5
MERGE INTO dim_customer t USING stg_customer s ON t.customer_id=s.customer_id WHEN MATCHED THEN UPDATE SET city=s.city,email=s.email WHEN NOT MATCHED THEN INSERT (customer_id,city,email) VALUES (s.customer_id,s.city,s.email);
-- 6
-- Expire changed current rows, then insert new versions as shown in SCD Type 2 section.
-- 7
SELECT batch_id, SUM(source_count) source_count, SUM(target_count) target_count, SUM(source_count)-SUM(target_count) diff FROM validation_counts GROUP BY batch_id;
-- 8
SELECT order_date, SUM(src_amount) src_amount, SUM(tgt_amount) tgt_amount, SUM(src_amount)-SUM(tgt_amount) diff FROM reconciliation_by_date GROUP BY order_date;
-- 9
SELECT COUNT(*) AS failed_count FROM orders WHERE customer_id IS NULL;
-- 10
SELECT order_id, COUNT(*) cnt FROM orders GROUP BY order_id HAVING COUNT(*)>1;
-- 11
SELECT f.* FROM fact_orders f LEFT JOIN dim_customer d ON f.customer_key=d.customer_key WHERE d.customer_key IS NULL;
-- 12
SELECT * FROM raw_orders WHERE order_id IS NULL OR try_cast(amount AS DECIMAL(18,2)) IS NULL;
-- 13
SELECT CAST(order_id AS BIGINT) order_id, CAST(order_ts AS TIMESTAMP) order_ts, CAST(amount AS DECIMAL(18,2)) amount FROM raw_orders WHERE order_id IS NOT NULL;
-- 14
SELECT order_date, COUNT(*) orders, SUM(amount) revenue FROM silver_orders GROUP BY order_date;
-- 15
DELETE FROM orders_target WHERE order_date=DATE '2026-10-05'; INSERT INTO orders_target SELECT * FROM orders_stage WHERE order_date=DATE '2026-10-05';
-- 16
SELECT * FROM events WHERE event_date >= CURRENT_DATE - INTERVAL '3 days';
-- 17
UPDATE watermarks SET last_watermark=(SELECT MAX(updated_at) FROM source) WHERE pipeline_name='orders';
-- 18
INSERT INTO audit(pipeline_name,row_count,run_ts) SELECT 'orders', COUNT(*), CURRENT_TIMESTAMP FROM stage;
-- 19
SELECT column_name FROM source_columns EXCEPT SELECT column_name FROM target_columns;
-- 20
SELECT s.key FROM src s JOIN tgt t ON s.key=t.key WHERE s.row_hash<>t.row_hash;
-- 21
UPDATE target SET is_deleted=true, deleted_at=CURRENT_TIMESTAMP WHERE NOT EXISTS (SELECT 1 FROM src_keys s WHERE s.key=target.key);
-- 22
INSERT INTO dim_customer(customer_key,customer_id,customer_name) VALUES (-1,'UNKNOWN','Unknown Customer');
-- 23
SELECT o.order_id, COALESCE(d.customer_key,-1) customer_key FROM orders o LEFT JOIN dim_customer d ON o.customer_id=d.customer_id;
-- 24
DELETE FROM monthly_revenue WHERE month_start BETWEEN DATE '2026-01-01' AND DATE '2026-03-01'; INSERT INTO monthly_revenue SELECT DATE_TRUNC('month',order_date),SUM(amount) FROM orders GROUP BY DATE_TRUNC('month',order_date);
-- 25
INSERT INTO fact_events SELECT s.* FROM stage_events s WHERE NOT EXISTS (SELECT 1 FROM fact_events f WHERE f.event_id=s.event_id);
-- 26
SELECT status, COUNT(*) FROM orders WHERE status NOT IN ('COMPLETE','CANCELLED','PENDING') GROUP BY status;
-- 27
SELECT file_name,batch_id,COUNT(*) row_count FROM raw GROUP BY file_name,batch_id;
-- 28
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY commit_ts DESC) rn FROM cdc) SELECT * FROM r WHERE rn=1;
-- 29
MERGE INTO tgt t USING src s ON t.key=s.key WHEN MATCHED AND t.row_hash<>s.row_hash THEN UPDATE SET attr=s.attr,row_hash=s.row_hash WHEN NOT MATCHED THEN INSERT (key,attr,row_hash) VALUES (s.key,s.attr,s.row_hash);
-- 30
SELECT 'missing_in_target' issue, * FROM src EXCEPT SELECT 'missing_in_target', * FROM tgt;
```

### Level 5 Solutions

For senior-level problems, interviewers care about the reasoning as much as syntax.

```sql
-- 1 Daily revenue mart
SELECT o.order_date, p.category, COUNT(DISTINCT o.order_id) orders, SUM(oi.quantity * oi.unit_price) revenue
FROM orders o JOIN order_items oi ON o.order_id=oi.order_id JOIN products p ON oi.product_id=p.product_id
GROUP BY o.order_date, p.category;

-- 2 Customer 360
SELECT c.customer_id, c.name, COUNT(DISTINCT o.order_id) orders, SUM(o.amount) revenue, MAX(e.event_ts) last_seen_ts
FROM customers c LEFT JOIN orders o ON c.customer_id=o.customer_id LEFT JOIN events e ON c.customer_id=e.user_id
GROUP BY c.customer_id, c.name;

-- 3 Revenue anomaly
WITH x AS (SELECT *, AVG(revenue) OVER (ORDER BY revenue_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) avg_prev_7 FROM daily_revenue)
SELECT * FROM x WHERE revenue < avg_prev_7 * 0.7;

-- 4 Reconcile snapshot and target
SELECT 'source_minus_target' issue, * FROM source_snapshot EXCEPT SELECT 'source_minus_target', * FROM target
UNION ALL
SELECT 'target_minus_source', * FROM target EXCEPT SELECT 'target_minus_source', * FROM source_snapshot;

-- 5 SCD2 null-safe change detection
SELECT s.* FROM stg_customer s JOIN dim_customer d ON s.customer_id=d.customer_id AND d.is_current=true
WHERE COALESCE(s.city,'~')<>COALESCE(d.city,'~') OR COALESCE(s.segment,'~')<>COALESCE(d.segment,'~');

-- 6 Historical SCD2 join
SELECT f.order_id, d.customer_key, d.segment FROM fact_orders f JOIN dim_customer d ON f.customer_id=d.customer_id AND f.order_date BETWEEN d.effective_start_date AND d.effective_end_date;

-- 7 Session funnel
SELECT session_id, MAX(CASE WHEN event_name='view' THEN 1 ELSE 0 END) viewed, MAX(CASE WHEN event_name='purchase' THEN 1 ELSE 0 END) purchased FROM session_events GROUP BY session_id;

-- 8 Rolling conversion
SELECT event_date, SUM(purchases) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / NULLIF(SUM(views) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) AS rolling_conversion FROM daily_funnel;

-- 9 Monthly retention
WITH c AS (SELECT user_id, DATE_TRUNC('month', signup_date) cohort_month FROM users), a AS (SELECT DISTINCT user_id, DATE_TRUNC('month', event_date) activity_month FROM events)
SELECT c.cohort_month, a.activity_month, COUNT(DISTINCT a.user_id) users FROM c JOIN a ON c.user_id=a.user_id GROUP BY c.cohort_month,a.activity_month;

-- 10 Broadcast hint example
SELECT /*+ BROADCAST(d) */ e.*, d.attr FROM large_events e JOIN small_dim d ON e.dim_id=d.dim_id;

-- 11 Duplicate cause
SELECT f.key, COUNT(*) joined_rows FROM fact f JOIN dim d ON f.key=d.key GROUP BY f.key HAVING COUNT(*)>1;

-- 12 Idempotent partition backfill
DELETE FROM target WHERE business_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-07';
INSERT INTO target SELECT * FROM stage WHERE business_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-07';

-- 13 Change type
SELECT COALESCE(s.id,t.id) id, CASE WHEN t.id IS NULL THEN 'insert' WHEN s.id IS NULL THEN 'delete' WHEN s.hash<>t.hash THEN 'update' ELSE 'same' END change_type FROM src s FULL OUTER JOIN tgt t ON s.id=t.id;

-- 14 DQ scorecard
SELECT COUNT(*) row_count, SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) null_order_id, COUNT(*)-COUNT(DISTINCT order_id) duplicate_order_count FROM orders;

-- 15 Latest CDC state
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY commit_ts DESC, sequence_no DESC) rn FROM cdc) SELECT * FROM r WHERE rn=1 AND op<>'D';

-- 16 Priority source dedupe
WITH r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY CASE source_system WHEN 'CRM' THEN 1 WHEN 'API' THEN 2 ELSE 3 END, updated_at DESC) rn FROM records) SELECT * FROM r WHERE rn=1;

-- 17 Fiscal month revenue
SELECT cal.fiscal_month, SUM(s.amount) revenue FROM sales s JOIN calendar cal ON s.order_date=cal.calendar_date GROUP BY cal.fiscal_month;

-- 18 Product hierarchy rollup requires recursive CTE or prebuilt hierarchy bridge; aggregate sales at leaf then join to ancestors.

-- 19 User path
SELECT user_id, STRING_AGG(event_name, ' > ' ORDER BY event_ts) AS path FROM events GROUP BY user_id;

-- 20 Bot users
SELECT user_id FROM events GROUP BY user_id, DATE_TRUNC('hour', event_ts) HAVING COUNT(*) > 1000;

-- 21 SLA breach
SELECT * FROM pipeline_runs WHERE status='FAILED' OR end_ts > expected_end_ts;

-- 22 Safety-delay watermark
SELECT * FROM source WHERE updated_at > :last_watermark AND updated_at <= CURRENT_TIMESTAMP - INTERVAL '10 minutes';

-- 23 Incremental aggregate merge
MERGE INTO daily_revenue t USING (SELECT order_date, SUM(amount) revenue FROM orders GROUP BY order_date) s ON t.order_date=s.order_date WHEN MATCHED THEN UPDATE SET revenue=s.revenue WHEN NOT MATCHED THEN INSERT (order_date,revenue) VALUES (s.order_date,s.revenue);

-- 24 Late fact correction
MERGE INTO facts t USING stage s ON t.order_id=s.order_id WHEN MATCHED THEN UPDATE SET amount=s.amount,status=s.status WHEN NOT MATCHED THEN INSERT (order_id,amount,status) VALUES (s.order_id,s.amount,s.status);

-- 25 Bridge table
SELECT order_id, promotion_id FROM order_promotions;

-- 26 LTV
SELECT customer_id, SUM(amount) AS lifetime_value FROM orders GROUP BY customer_id;

-- 27 Percentiles by endpoint
SELECT endpoint, percentile_approx(latency_ms, 0.5) p50, percentile_approx(latency_ms, 0.95) p95, percentile_approx(latency_ms, 0.99) p99 FROM api_events GROUP BY endpoint;

-- 28 Inventory snapshot
SELECT product_id, event_date, SUM(quantity_delta) OVER (PARTITION BY product_id ORDER BY event_date) AS stock_on_hand FROM inventory_events;

-- 29 Missing partitions
SELECT e.partition_date FROM expected_dates e LEFT JOIN loaded_partitions l ON e.partition_date=l.partition_date WHERE l.partition_date IS NULL;

-- 30 Optimization answer: inspect EXPLAIN, reduce columns/rows before joins, fix join cardinality, add partition filters, broadcast small dimensions, address skew, and validate with runtime metrics.
```

# SQL Data Engineer Cheat Sheet

## JOINs

```sql
SELECT *
FROM a
JOIN b ON a.key = b.key;
```

- Inner: matches only.
- Left: all left rows.
- Full: all rows from both sides.
- Cross: every combination.
- Anti-join: left join then `WHERE b.key IS NULL`.

## Aggregations

```sql
SELECT key, COUNT(*), SUM(amount)
FROM t
WHERE event_date >= DATE '2026-10-01'
GROUP BY key
HAVING COUNT(*) > 1;
```

## CTE

```sql
WITH cleaned AS (
  SELECT * FROM raw WHERE id IS NOT NULL
)
SELECT * FROM cleaned;
```

## Window Functions

```sql
ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC)
LAG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_ts)
SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

## Date Functions

```sql
CURRENT_DATE
DATE_TRUNC('month', order_date)
order_date >= CURRENT_DATE - INTERVAL '7 days'
```

## String Functions

```sql
LOWER(TRIM(email))
CONCAT(first_name, ' ', last_name)
REPLACE(phone, '-', '')
```

## CASE

```sql
CASE WHEN amount >= 100 THEN 'high' ELSE 'low' END
```

## NULL Handling

```sql
IS NULL
IS NOT NULL
COALESCE(value, fallback)
NULLIF(denominator, 0)
```

## Set Operations

```sql
UNION       -- dedupe
UNION ALL   -- keep all
INTERSECT   -- common rows
EXCEPT      -- rows in first not second
```

## MERGE

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.attr = s.attr
WHEN NOT MATCHED THEN INSERT (id, attr) VALUES (s.id, s.attr);
```

## Deduplication

```sql
WITH r AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY updated_at DESC) rn
  FROM staging
)
SELECT * FROM r WHERE rn = 1;
```

## SCD

- Type 1: overwrite.
- Type 2: expire old row, insert new current row.
- Type 2 columns: surrogate key, natural key, effective start, effective end, current flag.

## Incremental Loading

```sql
SELECT *
FROM source
WHERE updated_at > :last_watermark
  AND updated_at <= :current_watermark;
```

## CDC

- Apply inserts.
- Apply updates.
- Apply deletes or soft deletes.
- Order by commit timestamp and sequence.

## Query Optimization

- Avoid `SELECT *`.
- Filter early.
- Use partition filters.
- Validate join cardinality.
- Broadcast small dimensions in Spark.
- Watch shuffles and skew.
- Use `EXPLAIN`.
- Keep statistics fresh.
- Compact small files in Delta.

# What I Must Know as a Senior Data Engineer

### MUST KNOW

- Joins, especially duplicate behavior and unmatched records.
- Aggregations, `WHERE` vs `HAVING`, `COUNT(*)` vs `COUNT(column)`.
- Window functions: `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, running totals.
- CTEs for readable transformations.
- `MERGE` and upsert patterns.
- Incremental loads, watermarks, CDC basics.
- Deduplication with deterministic ordering.
- Data quality checks: nulls, duplicates, referential integrity, reconciliation.
- SCD Type 1 and Type 2.
- Star schema, facts, dimensions, grain.
- Partition pruning, predicate pushdown, query plans.
- Spark/Databricks basics: shuffle, broadcast join, Delta Lake, OPTIMIZE.

### SHOULD KNOW

- Gaps and islands.
- Sessionization.
- Retention, cohort, funnel analysis.
- Semi-structured data: JSON, arrays, explode.
- Transactions and ACID.
- Redshift distribution/sort key basics.
- Unity Catalog security concepts.
- Late-arriving data handling.
- Idempotent pipeline design.
- Source-target validation with `EXCEPT` and checksums.

### GOOD TO KNOW

- Recursive CTEs.
- Materialized views.
- Advanced indexing concepts.
- Percentiles and approximate aggregations.
- Data skew mitigation with salting.
- Calendar/date spine patterns.
- Schema drift detection.
- Fiscal calendar modeling.

### OPTIONAL

- Deep DBA internals.
- Lock tuning beyond practical pipeline impact.
- Vendor-specific optimizer hints beyond your platform.
- Complex recursive graph queries unless your domain needs them.
- Stored procedure-heavy designs unless your company uses them.

