# OLTP, OLAP, Data Lake, Data Warehouse, Delta Lake & Lakehouse
### Senior Data Engineer — Interview & Revision Notes

---

## 1. OLTP vs OLAP

### 1.1 What they are
- **OLTP (Online Transaction Processing):** systems built to run day-to-day business operations — booking, payment, order placement, account updates.
- **OLAP (Online Analytical Processing):** systems built to analyze large volumes of historical/business data.

> **OLTP = run the business. OLAP = analyze the business.**

### 1.2 Why we use each
- OLTP: the application layer needs fast, reliable, atomic single-record operations at high concurrency (thousands of simultaneous small transactions).
- OLAP: the business needs to ask aggregate questions over huge historical datasets (e.g. "total revenue by route last 12 months") — something OLTP schemas/engines are not optimized for.

### 1.3 How they work internally
| | OLTP | OLAP |
|---|---|---|
| Schema | Normalized (3NF) — avoids duplication, supports fast single-row writes | Denormalized — star/snowflake schema, optimized for scans & aggregation |
| Storage | Row-oriented (fetch a full row fast) | Column-oriented (scan one column across billions of rows fast) |
| Operations | INSERT/UPDATE/DELETE, small transactions | Mostly SELECT with GROUP BY/aggregation over large scans |
| Concurrency | High — many small concurrent transactions | Lower — fewer, heavier queries |
| Latency | Milliseconds | Seconds to minutes |

### 1.4 Examples
```sql
-- OLTP: single-row transactional write
INSERT INTO orders (order_id, customer_id, amount)
VALUES (101, 5001, 2500);

UPDATE accounts SET balance = balance - 500 WHERE account_id = 1001;

-- OLAP: large aggregation across historical data
SELECT flight_date, origin, destination, SUM(revenue)
FROM flight_sales
GROUP BY flight_date, origin, destination;
```

### 1.5 Normalization vs denormalization
- OLTP: `Customer` and `Order` kept separate, joined by `customer_id` — avoids repeating customer data in every order row, prevents update anomalies.
- OLAP: star schema — one central **fact table** (measures: revenue, quantity) surrounded by **dimension tables** (`Dim_Customer`, `Dim_Product`, `Dim_Date`, `Dim_Store`). Denormalized on purpose — fewer joins, faster scans, easier for BI tools.

### 1.6 Best practices / when to use which
- Never run heavy analytical aggregation directly on an OLTP production DB — it competes with live transactions and can lock/slow the application.
- Never use an OLAP warehouse for high-frequency single-row transactional writes — designed for bulk loads/scans, not row-level OLTP-style concurrency.
- Standard pattern: OLTP → ETL/ELT → OLAP, keeping the two workloads physically and operationally separate.

### 1.7 Common mistakes
- Building analytical dashboards straight off a normalized OLTP schema — leads to expensive multi-way joins and slow dashboards.
- Treating a warehouse like an OLTP store (row-by-row inserts/updates) — very inefficient on columnar engines; batch loads are the correct pattern.

### 1.8 Real-world scenario
- Airline booking app writes to PostgreSQL (OLTP) as customers book seats in real time. Nightly/streaming ETL moves this data into Redshift (OLAP) where analysts run revenue and route-performance reports — the two systems never share load.

### 1.9 Interview points
- Be ready to classify a given system/query as OLTP or OLAP and justify with schema/latency/workload reasoning.
- "Why not just run analytics on the production DB?" — resource contention, schema mismatch, no columnar scan efficiency.

### 1.10 Quick summary
> OLTP → transactions, normalized, row-store, millisecond latency, few rows.
> OLAP → analytics, denormalized (star schema), column-store, second/minute latency, millions/billions of rows.

---

## 2. Data Lake

### 2.1 What it is
An architecture for storing large volumes of data — structured, semi-structured, and unstructured — cheaply, typically on object storage (S3, ADLS, GCS), without forcing a rigid schema at write time.

### 2.2 Why we use it
- Very low-cost storage at massive scale (TB–PB+).
- Stores raw data in its original form as a durable source of truth before any transformation.
- One copy of data can serve multiple downstream needs — ETL, BI, ML, ad-hoc analysis — instead of duplicating data per use case.

### 2.3 How it works internally
- Data lands as files (CSV, JSON, Parquet, Avro, images, logs, etc.) in a folder hierarchy on object storage.
- No enforced table schema at write time — this is **schema-on-read**: the schema/interpretation is applied by whatever engine reads the data later (Spark, Athena, Presto).
- Example layout:
```text
S3
├── flights/flight_001.json
├── bookings/booking_001.csv
├── logs/application.log
└── customers/customer.parquet
```

### 2.4 Best practices
- Organize with a clear zone structure (raw/bronze → cleaned/silver → curated/gold — see Medallion Architecture).
- Maintain metadata/catalog (e.g. Glue Catalog, Hive Metastore, Unity Catalog) so data is discoverable, not just "somewhere in a bucket."
- Prefer columnar formats (Parquet) over CSV/JSON for anything downstream of raw ingestion — smaller, faster to scan.
- Apply naming/versioning conventions and access governance from day one.

### 2.5 Common mistakes — the "Data Swamp"
Without governance, a data lake degrades into a **data swamp**:
```text
data_final.csv
data_final_v2.csv
data_final_latest.csv
data_new.csv
unknown/
```
Problems: duplicate data, no metadata, inconsistent schemas, poor data quality, no transactional guarantees (partial writes visible), hard to discover or trust. This is the core motivation behind Delta Lake / lakehouse table formats.

### 2.6 How to identify problems
- Check catalog completeness — how much of the lake is actually registered/discoverable vs "unknown" paths.
- Check for duplicate/near-duplicate files with unclear naming (`_v2`, `_final`, `_latest`).
- Check whether concurrent writers can produce partial/corrupt reads (no ACID layer = real risk).

### 2.7 Real-world scenario
- Raw clickstream/log data lands in S3 as-is (schema-on-read) so both the ML team (needs raw granular events) and the analytics team (needs cleaned/aggregated tables) can build from the same source without re-collecting data.

### 2.8 Interview points
- Explain schema-on-read vs schema-on-write and the trade-off (flexibility vs governance/performance).
- Explain what causes a data swamp and how modern lakehouse tooling (Delta/Iceberg/Hudi + catalogs) fixes it.

### 2.9 Quick summary
> Data Lake = cheap, flexible, schema-on-read storage for any data type; without governance it becomes a data swamp — solved by lakehouse table formats + catalogs.

---

## 3. Data Warehouse

### 3.1 What it is
A platform purpose-built for structured, governed analytical workloads — e.g. Redshift, Snowflake, BigQuery, Synapse.

### 3.2 Why we use it
- Optimized SQL performance on large structured datasets (columnar storage, query optimizer, MPP execution).
- Strong governance, consistency, and schema control — reliable for BI/reporting where correctness matters.
- Purpose-fit for dimensional modelling (star/snowflake schemas) used by BI tools.

### 3.3 How it works internally
- Enforces **schema-on-write** — structure is defined and validated as data is loaded/modelled.
- Typically column-oriented storage + massively parallel processing (see Redshift internals, Section 8).
- Query optimizer plans execution using statistics, sort keys, and distribution strategy to minimize scanned/shuffled data.

### 3.4 Example architecture
```text
Operational Sources → ETL/ELT → Data Warehouse → BI / Reports / Analytics
```

### 3.5 Best practices
- Model with star/snowflake schemas for BI-friendly querying.
- Load in bulk (COPY-style bulk loads), not row-by-row — matches the columnar/MPP design.
- Keep raw/unstructured data out of the warehouse; land it in a lake first, curate into the warehouse.

### 3.6 Common mistakes
- Using the warehouse as a general-purpose raw data dumping ground — expensive storage, poor fit for unstructured/semi-structured data, and defeats schema-on-write governance.
- Ignoring distribution/sort key design (see Section 8) — leads to expensive shuffles and full scans even on a well-built warehouse.

### 3.7 Limitations
- More expensive as a primary raw-data repository than object storage.
- Less flexible for unstructured data and some ML workloads that want direct access to raw files.

### 3.8 Real-world scenario
- Finance team needs consistent, governed monthly revenue reports — data is curated and loaded into Redshift with enforced schema so BI dashboards never break due to unexpected structure changes.

### 3.9 Interview points
- Be ready to explain schema-on-write vs schema-on-read and why warehouses choose the former.
- Explain why raw/unstructured data usually lands in a lake, not directly in the warehouse.

### 3.10 Quick summary
> Data Warehouse = governed, schema-on-write, columnar/MPP platform optimized for structured SQL analytics and BI — strong on consistency/performance, weaker on flexibility/cost for raw data.

---

## 4. Data Lake vs Data Warehouse

| Feature | Data Lake | Data Warehouse |
|---|---|---|
| Purpose | Store diverse data (raw + processed) | Analyze structured data |
| Storage | Object storage (S3/ADLS/GCS) | Managed warehouse storage |
| Schema | Schema-on-read | Schema-on-write |
| Data types | Structured + semi/unstructured | Mostly structured |
| Cost | Lower storage cost | Higher |
| SQL / BI | Traditionally limited / good with engines on top | Excellent |
| ML fit | Excellent (raw access) | Possible but less natural |
| Governance | Traditionally weaker (unless lakehouse tooling used) | Stronger |
| Example | S3 | Redshift |

**Interview one-liner:** *"A Data Lake gives flexible, low-cost storage for raw and diverse data; a Data Warehouse gives governed, structured storage optimized for analytical SQL."*

---

## 5. Delta Lake

### 5.1 What it is
A **table/storage layer** built on top of object storage — **not** a data lake itself, and not a file format itself. It adds reliable table semantics on top of Parquet files.

> **Delta Lake ≠ Data Lake.** Delta Lake is the technology that makes a data lake behave like a reliable table.

### 5.2 Why we use it
Raw data lakes lack transactional guarantees, update/delete support, and schema safety — Delta Lake solves exactly these gaps (the root causes of the "data swamp" problem).

### 5.3 How it works internally
- Data is stored as **Parquet files**.
- A `_delta_log/` folder holds an ordered, versioned transaction log (JSON commit files) recording every change to the table.
```text
sales/
├── part-0001.parquet
├── part-0002.parquet
└── _delta_log/
    ├── 000000.json
    ├── 000001.json
    └── 000002.json
```
- Every write (insert/update/delete/merge) is recorded as a new atomic commit in the log — this is how ACID guarantees, time travel, and consistent reads are achieved on top of plain object storage (which has no native transactions).

### 5.4 What it provides
- **ACID transactions** — atomic, consistent, isolated, durable writes even with concurrent writers.
- **Time travel** — query previous versions of a table by version or timestamp.
- **UPDATE / DELETE / MERGE** (upserts) directly on the table — not possible on plain Parquet.
- **Schema enforcement** — rejects incompatible writes.
- **Schema evolution** — controlled, explicit schema changes.
- **Change Data Feed** — exposes row-level changes between versions (useful for CDC-style downstream pipelines).

### 5.5 How to use it — examples
```sql
UPDATE customers SET status = 'ACTIVE' WHERE customer_id = 101;

DELETE FROM customers WHERE customer_id = 101;

MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
```python
# Time travel
df = spark.read.format("delta").option("versionAsOf", 5).load("path")
df = spark.read.format("delta").option("timestampAsOf", "2026-01-01").load("path")
```

### 5.6 Best practices
- Run periodic `OPTIMIZE` (compaction) to fight the small-file problem from frequent small commits/streaming writes.
- Use `VACUUM` to clean up old, unreferenced files (after time-travel retention window) to control storage cost.
- Use `MERGE` for upserts/CDC ingestion instead of manual delete+insert logic.
- Enforce schema on write for critical tables; allow controlled evolution only where genuinely needed.

### 5.7 Common mistakes
- Treating Delta tables like plain Parquet and manually deleting/overwriting files outside Delta APIs — corrupts the transaction log.
- Never running `OPTIMIZE`/`VACUUM` — leads to small-file bloat and unbounded storage growth from old versions.
- Relying on time travel as a long-term backup strategy without understanding retention/vacuum interactions.

### 5.8 How to identify problems
- Check `_delta_log` size/commit frequency — very frequent tiny commits signal a small-file problem.
- `DESCRIBE HISTORY table` to audit versions, operations, and who/what changed data.
- `DESCRIBE DETAIL table` to check file count/size — many tiny files = compaction needed.

### 5.9 Real-world scenario
- CDC feed from an OLTP source is continuously `MERGE`d into a Delta silver table for upserts; analysts can time-travel to yesterday's version to debug a bad load without needing a separate backup system.

### 5.10 Interview points
- Parquet vs Delta Lake — file format vs table/storage layer (very common question).
- How does Delta achieve ACID on top of object storage that has no native transactions? → ordered, atomic commit log.
- What's the small-file problem in Delta and how do you fix it? → `OPTIMIZE`/compaction.

### 5.11 Quick summary
> Delta Lake = Parquet files + a transaction log, giving ACID, time travel, MERGE/UPDATE/DELETE, and schema control on top of a plain data lake.

---

## 6. Parquet vs Delta Lake — don't confuse these
| | Parquet | Delta Lake |
|---|---|---|
| What it is | File format | Table/storage layer |
| Provides | Columnar, compressed physical storage | ACID, time travel, MERGE/UPDATE/DELETE, schema enforcement, on top of Parquet |
| Transactions | None | Yes (via `_delta_log`) |
| Relationship | Used *by* Delta Lake as the data file format | Uses Parquet + adds a transaction log |

**Interview one-liner:** *"Parquet is a file format; Delta Lake is a table layer that uses Parquet files plus a transaction log to add reliability."*

---

## 7. Lakehouse

### 7.1 What it is
An architecture that combines Data Lake flexibility/economics with Data Warehouse-style reliability, governance, and SQL performance — typically Delta Lake (or Iceberg/Hudi) on top of object storage.

### 7.2 Why we use it
Avoids maintaining two separate copies of data (one in a lake for ML/raw, one in a warehouse for BI) — a single governed copy serves data engineering, SQL analytics, BI, and ML/AI.

### 7.3 How it works — Databricks Lakehouse example
```text
Databricks Lakehouse
        │
 ┌──────┼──────┐
Data Eng   SQL     ML/AI
 (Spark) (SQL Engine) (ML)
        │
   Delta Lake
        │
        S3
```
The same underlying Delta tables support Spark ETL, SQL analytics/BI, and ML/AI workloads — no data duplication across separate systems.

### 7.4 Medallion Architecture (common lakehouse pattern)
```text
S3 → Bronze (raw/ingested) → Silver (cleaned/validated) → Gold (business aggregates) → BI / Analytics / ML
```
- **Bronze:** raw, as-ingested data (often append-only, minimal transformation).
- **Silver:** cleaned, deduplicated, validated, joined/conformed data.
- **Gold:** business-level aggregates and curated datasets ready for consumption.

### 7.5 Best practices
- Keep bronze immutable/append-only as the auditable source of truth.
- Apply data quality checks and dedup logic at silver, not gold.
- Model gold tables around actual consumption patterns (dashboards, ML features) — not just "cleaner bronze."
- Use a catalog (Unity Catalog/Glue/Hive Metastore) across all three layers for governance and lineage.

### 7.6 Common mistakes
- Skipping the medallion layering and writing straight to "gold-like" curated tables — loses auditability and reprocessing flexibility.
- Mixing raw and curated data in the same table/location — breaks the "reprocess bronze if silver logic changes" pattern.

### 7.7 Real-world scenario
- A single Delta-based lakehouse on Databricks serves: nightly Spark ETL (bronze→silver→gold), BI dashboards querying gold via SQL warehouse, and a data science team training models directly off silver — all from one governed copy of the data instead of three separate systems.

### 7.8 Interview points
- Explain why lakehouse architecture emerged (data swamp + two-copy-of-data problem with separate lake/warehouse).
- Be ready to describe bronze/silver/gold responsibilities precisely (this is asked often).

### 7.9 Quick summary
> Lakehouse = Data Lake storage + Delta Lake table reliability + Warehouse-style SQL/BI/governance, unified for Data Engineering, BI, and ML on one copy of data — commonly organized via Bronze → Silver → Gold.

---

## 8. Amazon Redshift Internals (relevant OLAP/warehouse deep-dive)

### 8.1 What it is
A cloud data warehouse using a **Massively Parallel Processing (MPP)** architecture with a Leader Node coordinating multiple Compute Nodes, each split into Slices.

### 8.2 Why it matters
Understanding distribution/sort keys and columnar storage is what determines whether Redshift queries are fast (local, pruned) or slow (full scans, cross-node shuffles).

### 8.3 How it works internally
```text
Redshift
  Leader Node → parses SQL, plans query, coordinates, returns results
  Compute Nodes → do the actual parallel processing
    Slices → each compute node divided into slices; data distributed across slices for parallelism
```
- **MPP:** instead of one machine scanning 1 billion rows, the workload is split across many slices working in parallel.
- **Columnar storage:** data stored column-by-column rather than row-by-row, so `SELECT AVG(salary) FROM employees` only reads the `salary` column, not the whole row — much less I/O for analytical aggregations.

### 8.4 Distribution styles
| Style | Behavior | When to use |
|---|---|---|
| `DISTKEY(col)` | Rows distributed by hash of the key column | Large tables frequently joined on that key — colocates matching rows to avoid cross-node shuffle |
| `EVEN` | Rows spread evenly, round-robin | No good join key, or table not frequently joined |
| `ALL` | Full copy of the table on every node | Small reference/dimension tables (e.g. `country`, `calendar`) — avoids redistribution entirely on join |
| `AUTO` | Redshift chooses automatically | Default when unsure; Redshift adapts as table grows |

```sql
CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL(10,2)
)
DISTKEY(customer_id);
```

### 8.5 Why distribution choice matters — join example
```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
```
- If `orders` and `customers` are distributed by `customer_id`, matching rows sit on the **same slice** → local join, fast.
- If distributed differently, Redshift must **redistribute** data across nodes over the network before joining → expensive shuffle, slow query.

### 8.6 Sort keys
```sql
CREATE TABLE sales (
    sale_id BIGINT, customer_id BIGINT, sale_date DATE, amount DECIMAL(10,2)
)
SORTKEY(sale_date);
```
- Physically orders data on disk by the sort key, enabling **zone maps** (block-level min/max metadata) so Redshift can skip blocks that can't match a filter — similar in spirit to partition pruning in Spark/Hive.
- Best for columns frequently used in range filters (`WHERE sale_date BETWEEN ...`) or `ORDER BY`.

### 8.7 Best practices
- Pick `DISTKEY` = the most common large-table join column; use `ALL` only for genuinely small dimension tables.
- Pick `SORTKEY` = the most common range-filter column (often a date).
- Use `AUTO` distribution/sort when uncertain, and validate via `EXPLAIN` and query performance later.
- Load in bulk via `COPY`, not row-by-row inserts (matches the MPP/columnar design).

### 8.8 Common mistakes
- Wrong/missing `DISTKEY` on large frequently-joined tables → constant cross-node redistribution, slow joins.
- Using `ALL` distribution on a large table → wastes storage (full copy per node) and slows writes.
- No `SORTKEY` on a large fact table with frequent date-range queries → full table scans instead of block skipping.

### 8.9 How to identify problems
- `EXPLAIN` the query — look for `DS_DIST_*` operators (e.g. `DS_DIST_BOTH`, `DS_DIST_INNER`) indicating expensive redistribution during joins.
- Check `SVL_QUERY_SUMMARY` / `STL_ALERT_EVENT_LOG` (Redshift system tables) for redistribution and broadcast costs.
- Check skew across slices — uneven storage/query time per slice signals a bad `DISTKEY` choice.

### 8.10 Real-world scenario
- A `fact_sales` table (billions of rows) distributed by `customer_id` to colocate with a large `dim_customer` join; `dim_country` (small, static) distributed as `ALL` since it's joined everywhere and rarely changes; `SORTKEY(sale_date)` since nearly every query filters by date range.

### 8.11 Interview points
- Explain `DISTKEY` vs `SORTKEY` — distribution decides *where* rows live across nodes; sort decides *ordering within* each node's storage for scan efficiency.
- Explain what happens when join keys don't match distribution keys (redistribution cost).
- Explain why columnar storage benefits aggregate queries specifically.

### 8.12 Quick summary
> Redshift = MPP + columnar. `DISTKEY` controls cross-node data placement (colocate joins), `SORTKEY` controls on-disk ordering (block skipping via zone maps), `ALL` for small dimension tables, `AUTO` when unsure.

---

## 9. Redshift ↔ S3 movement: COPY and UNLOAD

| Command | Direction | Purpose |
|---|---|---|
| `COPY` | S3 → Redshift | Bulk-load data into a Redshift table |
| `UNLOAD` | Redshift → S3 | Export query results out to S3 (e.g. as Parquet) |

```sql
-- COPY: S3 into Redshift
COPY target_table
FROM 's3://bucket/path/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;

-- UNLOAD: Redshift out to S3
UNLOAD ('SELECT * FROM source_table')
TO 's3://bucket/output/'
IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftRole'
FORMAT AS PARQUET;
```
**Migration relevance:** in a Redshift → Databricks migration, `UNLOAD` moves data from Redshift to S3 as Parquet, which Spark then reads and writes into Delta tables.

---

## 10. Putting it all together — Redshift → Databricks migration mental model

```text
Redshift (Data Warehouse, OLAP, MPP)
    │  UNLOAD
    ↓
S3 (Data Lake storage)
    │  Parquet (file format)
    ↓
Databricks (Spark reads Parquet)
    │  write
    ↓
Delta Lake (table/storage layer: ACID, time travel, MERGE)
    │  Bronze → Silver → Gold
    ↓
Lakehouse (Data Eng + SQL/BI + ML/AI on one governed copy)
```

| Concept | Category | Role in migration |
|---|---|---|
| Redshift | Data Warehouse (OLAP) | Source system |
| S3 | Data Lake storage | Intermediate/target storage |
| Parquet | File format | Format used during data movement |
| Delta Lake | Table/storage layer | Target table technology |
| Databricks | Lakehouse platform | Target platform for Eng/SQL/ML |

---

## SENIOR-LEVEL CHECKLIST
- [ ] Can classify any given system/query as OLTP or OLAP with schema/latency reasoning.
- [ ] Can explain schema-on-read vs schema-on-write and why each architecture picks one.
- [ ] Can clearly separate: Data Lake (storage architecture) vs Data Warehouse (analytical platform) vs Delta Lake (table layer) vs Lakehouse (combined architecture).
- [ ] Knows Parquet is a file format, not a table technology — Delta Lake is the table technology.
- [ ] Can explain how Delta Lake achieves ACID on plain object storage (`_delta_log`).
- [ ] Understands Bronze/Silver/Gold responsibilities precisely.
- [ ] Can explain Redshift `DISTKEY` vs `SORTKEY` and diagnose a bad distribution choice via `EXPLAIN`.
- [ ] Can walk through the Redshift → S3 → Databricks/Delta migration flow end-to-end (COPY/UNLOAD, Parquet, Delta, Medallion).

## COMMON INTERVIEW QUESTIONS
1. Difference between OLTP and OLAP — schema, storage, latency, workload?
2. Data Lake vs Data Warehouse — when would you use each, and can they coexist?
3. What is Delta Lake, and how is it different from Parquet?
4. How does Delta Lake provide ACID transactions on top of object storage?
5. What is a Lakehouse, and why did it emerge as an architecture?
6. Explain Bronze/Silver/Gold and what belongs in each layer.
7. In Redshift, what's the difference between `DISTKEY` and `SORTKEY`?
8. What happens if a join's key doesn't match the table's distribution key in Redshift?
9. What is a "data swamp" and how do you prevent one?
10. Walk through how you'd migrate a table from Redshift to a Databricks Lakehouse.

## KEY POINTS TO REMEMBER
1. OLTP = transactions, normalized, row-store; OLAP = analytics, denormalized (star schema), column-store.
2. Data Lake = flexible, cheap, schema-on-read storage; Data Warehouse = governed, schema-on-write analytical platform.
3. Parquet = file format. Delta Lake = table/storage layer (Parquet + transaction log).
4. Delta Lake adds ACID, time travel, MERGE/UPDATE/DELETE, schema enforcement/evolution on top of a plain lake.
5. Lakehouse = Data Lake flexibility + Warehouse-style reliability/governance/SQL, unified via Delta Lake.
6. Medallion architecture: Bronze (raw) → Silver (cleaned/validated) → Gold (business-ready aggregates).
7. Ungoverned data lakes become data swamps — duplicate files, no metadata, no transactional guarantees.
8. Redshift is MPP + columnar: `DISTKEY` controls cross-node data placement (join colocation), `SORTKEY` enables block skipping via zone maps.
9. Mismatched distribution keys cause expensive cross-node redistribution during joins — check via `EXPLAIN`.
10. Typical migration flow: Redshift (`UNLOAD`) → S3/Parquet → Spark/Databricks → Delta Lake → Lakehouse (Bronze/Silver/Gold).