# File Formats And Compression Techniques

## 1. Why File Format Choice Matters

When designing a data lake or Spark pipeline, one important architecture question is:

```text
How will the data be stored?
```

File format affects:

- storage cost
- read performance
- write performance
- compression ratio
- schema evolution
- splittability
- parallelism
- predicate pushdown
- column pruning
- query cost in cloud engines

In cloud query engines such as Athena or serverless SQL engines, cost is often based on data scanned. Better file formats directly reduce cost.

## 2. Two Broad Categories

File formats can be grouped as:

1. Row-based formats
2. Column-based formats

## 3. Row-Based Formats

In row-based formats, all columns of one row are stored together.

Concept:

```text
row1: order_id, order_date, customer_id, order_status
row2: order_id, order_date, customer_id, order_status
row3: order_id, order_date, customer_id, order_status
```

Advantages:

- faster writes
- good for record-level inserts
- simple for full-row reads

Disadvantages:

- inefficient when reading only a few columns
- less compression compared with columnar formats
- more I/O for analytical queries

Examples:

- CSV
- JSON
- XML
- Avro

## 4. Column-Based Formats

In column-based formats, values of the same column are stored together.

Concept:

```text
order_id column:      1, 2, 3, 4
order_date column:    2013-07-25, 2013-07-25, ...
customer_id column:   11599, 256, 12111, ...
order_status column:  CLOSED, PENDING_PAYMENT, COMPLETE, ...
```

Advantages:

- efficient reads for selected columns
- strong compression
- supports predicate pushdown
- ideal for analytics

Disadvantages:

- slower writes than row-based formats
- not ideal for frequent single-row updates

Examples:

- Parquet
- ORC

## 5. Text Formats

Text formats are easy to inspect and useful for learning, but they are usually not ideal for large-scale analytics.

### CSV

Problems:

- everything is stored as text
- numeric/date values must be parsed
- higher storage usage
- more I/O
- no embedded schema
- weak support for schema evolution

Example:

```text
90123490
```

In CSV this is text. Spark must convert it to integer or long before numeric operations.

### JSON And XML

JSON and XML have more structure than CSV, but they are bulky because field names or tags are stored repeatedly.

Problems:

- large file size
- parsing overhead
- often not splittable in multiline form
- more CPU cost
- more I/O

Single-line JSON can be easier for Spark to split than multiline JSON.

Example:

```python
orders_json_df = spark.read \
    .format("json") \
    .load("/public/trendytech/datasets/json_sample_singleline")
```

Multiline JSON:

```python
orders_json_ml_df = spark.read \
    .format("json") \
    .option("multiLine", True) \
    .load("/public/trendytech/datasets/json_sample_multiline")
```

## 6. Specialized Big Data Formats

Main formats:

| Format | Type | Best Fit |
|---|---|---|
| Avro | Row-based | landing zone, streaming, schema evolution |
| Parquet | Column-based | Spark analytics |
| ORC | Column-based | Hive-style analytics |

All three:

- are splittable
- support schema evolution
- store metadata
- work with compression codecs
- are better for big data than raw text formats

## 7. Avro

Avro is row-based and self-describing.

Strengths:

- fast writes
- schema stored with data
- strong schema evolution support
- good for landing/raw zones
- good for Kafka and streaming ecosystems
- splittable

Use Avro when:

- data is write-heavy
- schema evolves frequently
- you need a general-purpose serialized format
- data is used before heavy analytical modeling

## 8. Parquet

Parquet is column-based and very compatible with Spark.

Strengths:

- excellent analytical read performance
- strong compression
- column pruning
- predicate pushdown
- embedded metadata
- splittable
- schema evolution support

Use Parquet when:

- Spark is the primary processing engine
- workloads are analytical
- queries read selected columns
- storage and scan cost matter

## 9. ORC

ORC means Optimized Row Columnar.

It is columnar and heavily optimized, especially in Hive ecosystems.

Strengths:

- very strong compression
- excellent read performance
- predicate pushdown
- column pruning
- splittable
- schema evolution support

Use ORC when:

- Hive ecosystem compatibility matters
- storage optimization is a top priority
- workloads are analytical

## 10. Parquet Internal Structure

A Parquet file contains:

```text
Header: PAR1
Body:
  Row groups
    Column chunks
      Pages
Footer:
  Metadata
```

Concept:

```text
Parquet file
  |
  +-- Row group 1
  |     +-- order_id column chunk
  |     +-- order_date column chunk
  |     +-- customer_id column chunk
  |     +-- order_status column chunk
  |
  +-- Row group 2
        +-- order_id column chunk
        +-- order_date column chunk
        +-- customer_id column chunk
        +-- order_status column chunk
```

Each page can store statistics such as:

- min value
- max value
- null count
- encoding information

## 11. Predicate Pushdown In Parquet

Suppose a row group has:

```text
order_id min = 28
order_id max = 20045
```

Query:

```sql
SELECT *
FROM orders
WHERE order_id = 45
```

Spark can use metadata to decide whether the row group might contain the value.

If a row group has:

```text
order_id min = 50000
order_id max = 90000
```

Spark can skip it.

This is predicate pushdown/data skipping.

## 12. Column Pushdown

Query:

```sql
SELECT order_id, customer_id
FROM orders
```

With Parquet or ORC, Spark can read only:

```text
order_id column chunk
customer_id column chunk
```

It does not need to read `order_date` and `order_status`.

## 13. Lightweight Encodings

Columnar formats use encodings before or along with general compression.

### 13.1 Dictionary Encoding

Good when repeated values exist.

Example:

```text
Karnataka -> 1
Andhra Pradesh -> 2
Maharashtra -> 3
```

Instead of storing a long string repeatedly, Spark stores dictionary IDs.

### 13.2 Bit Packing

Stores values using as few bits as possible.

Example:

If dictionary IDs need only 5 bits, there is no need to store them as full 32-bit integers.

### 13.3 Delta Encoding

Stores differences between values rather than full values.

Good for:

- timestamps
- sequential IDs
- sorted numeric columns

Example:

```text
12:49:00
12:49:01
12:49:02
```

Can be stored as:

```text
base: 12:49:00
deltas: 1, 2
```

### 13.4 Run-Length Encoding

Stores repeated values as value plus count.

Example:

```text
ssssssssggggggghhhhhhhhh
```

Compressed:

```text
s8g7h9
```

For a column with one million zeroes:

```text
(0, 1000000)
```

## 14. General Compression Techniques

| Codec | Compression Ratio | Speed | Splittable With Text? | Typical Use |
|---|---|---|---|---|
| Snappy | Moderate | Fast | No | Default for Parquet/ORC |
| LZO | Moderate | Fast | Yes | Speed-focused Hadoop workloads |
| Gzip | Good | Moderate/slow | No | Storage saving, not ideal for CSV parallelism |
| Bzip2 | Very good | Slow | Yes | Archival |

## 15. Snappy

Snappy is optimized for speed.

Good:

- fast read/write
- moderate compression
- default for Parquet and ORC in many systems

Warning:

Snappy-compressed CSV/text files are often not splittable.

With Parquet/ORC, the container format preserves parallelism.

## 16. Gzip

Gzip gives better compression than Snappy but is slower and not splittable for plain text files.

Use Gzip when:

- storage saving matters more than speed
- files are not processed frequently
- used inside splittable container formats

Avoid Gzip on huge single CSV files if parallelism matters.

## 17. Bzip2

Bzip2 provides very strong compression and is splittable, but it is slow.

Use case:

- archival data
- rarely accessed data
- storage cost is more important than processing speed

## 18. Example Size Comparison

Illustrative comparison:

```text
CSV raw            -> 1.1 GB
CSV + Snappy       -> 313 MB, not splittable
CSV + Gzip         -> 168 MB, not splittable
CSV + Bzip2        -> 139 MB, splittable
Parquet + Snappy   -> 109 MB, splittable
Parquet + Gzip     -> 101 MB, splittable
ORC + Snappy       -> 59 MB, splittable
ORC + LZO          -> 52 MB, splittable
```

The lesson:

```text
Best compression is not always best performance.
```

## 19. Writing Formats In Spark

```python
orders_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .save(f"/user/{username}/orders_parquet_snappy")
```

```python
orders_df.write \
    .format("orc") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .save(f"/user/{username}/orders_orc_snappy")
```

```python
orders_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("compression", "gzip") \
    .save(f"/user/{username}/orders_csv_gzip")
```

## 20. Common Mistakes

1. Using CSV for repeated analytical queries.
2. Compressing one huge CSV with Gzip and losing parallelism.
3. Choosing maximum compression for frequently queried data.
4. Ignoring schema evolution requirements.
5. Reading all columns from a wide table.
6. Using JSON for large analytics when Parquet would fit better.
7. Assuming smaller file size always means faster query.

## 21. Production Guidance

Recommended pattern:

```text
Landing/raw zone       -> Avro or JSON if needed by ingestion constraints
Clean/curated zone     -> Parquet or ORC
Analytics/serving zone -> Parquet/ORC with partitioning and compression
Archive zone           -> stronger compression if rarely read
```

For Spark-heavy platforms, Parquet with Snappy is often a strong default.

## 22. Interview Questions

### Beginner

1. What is the difference between row-based and column-based formats?
2. Why is CSV not ideal for big data processing?
3. What is Parquet?
4. What is ORC?
5. What is Avro?

### Intermediate

1. Why does Parquet support column pruning?
2. How does predicate pushdown work?
3. Why is Gzip bad for huge CSV files?
4. What is dictionary encoding?
5. Why is Snappy popular?

### Senior

1. How would you choose file format for a lakehouse architecture?
2. When would you choose Avro over Parquet?
3. How do compression and splittability affect Spark parallelism?
4. How can file format reduce cloud query cost?
5. What tradeoffs exist between ORC and Parquet?

## 23. Quick Revision

- CSV/JSON are easy but inefficient at scale.
- Avro is row-based and good for landing/streaming/schema evolution.
- Parquet is columnar and excellent with Spark.
- ORC is columnar and strong in Hive-style ecosystems.
- Columnar formats support column pruning and predicate pushdown.
- Snappy is fast; Gzip compresses more but is not splittable for text.
- Best production default for Spark analytics is often Parquet + Snappy.
