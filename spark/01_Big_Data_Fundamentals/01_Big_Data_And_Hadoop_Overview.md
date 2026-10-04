# Understanding Big Data: The Big Picture

## Introduction

Big Data means data that is too large, too fast, too varied, or too complex for a single traditional system to store, process, and analyze comfortably.

The important point is not only "large data". A table with 5 TB of old records may be Big Data because one database server cannot process it quickly. A stream of payment events may also be Big Data even if each event is small, because the data is arriving continuously and decisions must be made quickly.

As a Big Data Engineer, my job is usually to build systems that:

- Collect data from many sources.
- Store raw and processed data safely.
- Process the data using distributed compute.
- Serve clean data to analysts, dashboards, ML models, APIs, or downstream applications.
- Make sure pipelines are reliable, scalable, monitored, and cost controlled.

Simple big picture:

```text
Data Sources
    |
    v
Ingestion Layer
    |
    v
Storage Layer
    |
    v
Processing Layer
    |
    v
Serving Layer
    |
    v
Reports / Dashboards / ML / Applications
```

In production, Big Data Engineering is not just about writing Spark code. It is about designing the full data flow end to end.

## Why Do We Need It?

Traditional systems worked well when:

- Data size was manageable.
- Data was mostly structured tables.
- Processing could happen on one strong server.
- Reports could wait for nightly batch jobs.

Modern systems are different.

Examples:

- An e-commerce company tracks clicks, searches, carts, payments, refunds, delivery status, and customer support chats.
- A bank tracks transactions, fraud signals, KYC documents, risk scores, and audit logs.
- A ride-sharing app tracks driver locations, trip events, pricing, traffic, payments, and ratings.
- A manufacturing company tracks sensor data from thousands of machines.

One machine cannot easily handle this kind of scale.

That is why Big Data systems use distributed architecture. Instead of depending on one very powerful computer, we use many computers working together.

Analogy:

If one person has to count 1 billion records, it will take a long time. If 100 people split the work and each counts a smaller part, the work finishes faster. Distributed systems follow the same idea.

## What Is Big Data?

IBM popularized Big Data using the idea of the V's. Most interviews start with 3 V's, but senior interviews often expect 5 V's or more.

### The 5 V's Of Big Data

#### 1. Volume

Volume means the amount of data.

If the data is so large that it cannot be stored or processed efficiently on a single conventional machine, it becomes a Big Data problem.

Examples:

- Social media posts, likes, and comments.
- Sensor data from IoT devices.
- Clickstream data from websites.
- Historical banking transactions.
- Application logs from thousands of servers.

Why it matters:

- Storage must be distributed.
- Processing must be parallel.
- Backup and recovery become more complex.
- Cost becomes a serious design factor.

#### 2. Variety

Variety means data comes in different formats.

Main types:

- Structured data: Has a fixed schema, usually rows and columns.
- Semi-structured data: Has some structure but is flexible.
- Unstructured data: No fixed tabular structure.

Examples:

```text
Structured:
    MySQL table, Oracle table, SQL Server table

Semi-structured:
    CSV, JSON, XML, Avro, Parquet

Unstructured:
    Images, videos, audio files, PDFs, log text, emails
```

Why it matters:

- A normal relational database is best for structured data.
- A data lake can store all formats.
- Processing logic changes based on the format.
- Schema evolution becomes important.

#### 3. Velocity

Velocity means the speed at which data arrives and must be processed.

Example:

During a big sale, an e-commerce company may want to know:

- How many iPhones were sold in the last 1 hour?
- Which payment methods are failing right now?
- Which region is showing high cart abandonment?
- Is fraud increasing in real time?

This is not only about storing data. The system must process fresh data quickly.

Velocity creates two common processing patterns:

- Batch processing: Process data in fixed intervals, like hourly or daily.
- Stream processing: Process data continuously as events arrive.

#### 4. Veracity

Veracity means data quality and correctness.

Real-world data is messy.

Examples:

- Missing customer ID.
- Invalid date format.
- Duplicate records.
- Negative quantity in an order table.
- Sensor sends impossible temperature.
- Different systems use different country codes.

Why it matters:

- Bad data creates wrong business decisions.
- Pipelines must validate and clean data.
- Data quality checks are production requirements, not optional polish.

#### 5. Value

Value means useful business outcome from data.

Collecting data is not enough. Data must help answer questions, reduce risk, increase revenue, improve customer experience, or automate decisions.

Examples:

- Fraud detection.
- Customer churn prediction.
- Product recommendation.
- Inventory planning.
- Revenue dashboards.
- Operational alerts.

Important interview point:

Big Data is not valuable because it is big. It is valuable only when we can convert it into trusted insight or action.

## Monolithic Vs Distributed Systems

### What Is A Resource?

A resource is something a computer uses to do work.

Common resources:

- CPU: Executes instructions.
- Memory or RAM: Holds data while processing.
- Storage: Holds data permanently.
- Network: Moves data between machines.

Example machine:

```text
CPU:     4 cores
Memory: 8 GB RAM
Storage: 1 TB disk
```

### Monolithic System

A monolithic system is one large machine or one tightly coupled system that holds all resources.

Scaling a monolithic system usually means vertical scaling.

Vertical scaling means increasing the power of one machine.

Example:

```text
Before:
    4 CPU cores, 8 GB RAM, 1 TB disk

After vertical scaling:
    32 CPU cores, 128 GB RAM, 10 TB disk
```

Problem:

Performance does not keep increasing in the same ratio forever.

If I double resources, I may not always get double performance because:

- Hardware has limits.
- Disk I/O may become bottleneck.
- Network may become bottleneck.
- Database locks may slow concurrent users.
- One machine can still fail and bring everything down.
- Bigger machines are very expensive.

### Distributed System

A distributed system is a group of machines working together like one system.

Each machine is called a node.

```text
Distributed Cluster

Node 1: CPU + RAM + Disk
Node 2: CPU + RAM + Disk
Node 3: CPU + RAM + Disk
Node 4: CPU + RAM + Disk
```

Scaling a distributed system usually means horizontal scaling.

Horizontal scaling means adding more machines.

Example:

```text
5 node cluster
    |
    v
Need more capacity?
    |
    v
Add 5 more nodes
```

Why Big Data systems prefer distributed architecture:

- Can store huge data across many machines.
- Can process data in parallel.
- Can scale by adding nodes.
- Can tolerate some machine failures.
- Can use commodity hardware or cloud instances.

Important note:

Distributed systems are powerful, but they are not magic. They introduce new problems like network failure, data skew, coordination, retries, and consistency.

## Designing A Big Data System

Any Big Data system must answer three basic questions.

### 1. Where Will The Data Be Stored?

Traditional storage cannot always hold petabytes of data cheaply.

Big Data storage must support:

- Large scale.
- Fault tolerance.
- Multiple file formats.
- Parallel access.
- Low cost.

Examples:

- HDFS.
- Amazon S3.
- Azure Data Lake Storage Gen2.
- Google Cloud Storage.
- Databricks Delta Lake on cloud object storage.

### 2. How Will The Data Be Processed?

If data is distributed across many machines, normal single-machine code is not enough.

We need distributed processing.

Examples:

- MapReduce.
- Apache Spark.
- Apache Flink.
- Databricks.
- AWS Glue.
- Azure Synapse.

### 3. Is The System Scalable?

The system should handle growth.

Growth can happen in:

- Data volume.
- Number of users.
- Number of pipelines.
- Freshness requirements.
- Number of dashboards.
- ML workloads.

Scalable design means the system can grow without rewriting everything.

## Hadoop Overview

### What Is Hadoop?

Hadoop was one of the first popular frameworks built to solve Big Data problems.

It is called a framework or ecosystem because it is not one single tool. It is a group of tools that solve storage, processing, resource management, querying, scheduling, and data movement problems.

Hadoop became popular because companies needed a cheaper way to store and process massive data using clusters of machines.

### Hadoop Evolution

Course timeline:

```text
2007: Hadoop 1.0
2012: Hadoop 2.0
Current major generation: Hadoop 3.x
```

Interview note:

Many modern companies have moved from classic Hadoop clusters to cloud data lakes and Spark platforms, but Hadoop concepts are still important because Spark, Hive, HDFS, YARN, and distributed thinking came from this world.

## Hadoop Architecture

Hadoop has three core components:

```text
Hadoop
  |
  |-- HDFS       -> Distributed storage
  |-- MapReduce  -> Distributed processing
  |-- YARN       -> Resource management
```

### HDFS

HDFS stands for Hadoop Distributed File System.

It stores large files by splitting them into blocks and distributing those blocks across multiple machines.

Default HDFS block size is commonly 128 MB in modern Hadoop.

That means a 500 MB file may be split into blocks like this:

```text
File size: 500 MB

Block 1: 128 MB
Block 2: 128 MB
Block 3: 128 MB
Block 4: 116 MB
```

Simple diagram:

```text
Large File
   |
   v
Split into blocks
   |
   +-- Block 1 -> DataNode 1
   +-- Block 2 -> DataNode 2
   +-- Block 3 -> DataNode 3
   +-- Block 4 -> DataNode 1
```

Why HDFS is useful:

- Stores very large files.
- Distributes data across nodes.
- Replicates blocks for fault tolerance.
- Allows processing near the data.

Replication factor is the number of copies HDFS keeps for each block.

Default replication factor is commonly 3.

```text
Block 1
  |
  +-- Copy 1 -> DataNode 1
  +-- Copy 2 -> DataNode 2
  +-- Copy 3 -> DataNode 3
```

Why replication is needed:

- If one DataNode fails, data is still available from another copy.
- HDFS can repair missing replicas automatically.
- Read performance can improve because clients may read from a nearby replica.

### MapReduce

MapReduce is the original Hadoop processing model.

It processes data in two main phases:

- Map: Break data into key-value pairs or intermediate records.
- Reduce: Group and aggregate the intermediate results.

Example idea:

Count words in a large file.

```text
Input:
    big data big spark

Map output:
    big -> 1
    data -> 1
    big -> 1
    spark -> 1

Reduce output:
    big -> 2
    data -> 1
    spark -> 1
```

Why MapReduce became less popular:

- Usually requires lots of Java code.
- Writes intermediate data to disk many times.
- Slower for iterative processing.
- Harder to develop and debug than Spark.

Important point:

MapReduce is mostly considered legacy for new development, but the idea of distributed map and reduce is still useful.

### YARN

YARN stands for Yet Another Resource Negotiator.

YARN is Hadoop's resource manager.

It decides which application gets CPU and memory in the cluster.

Example:

```text
20 node Hadoop cluster
    |
    +-- User A submits Spark job
    +-- User B submits Hive query
    +-- User C submits MapReduce job
    |
    v
YARN allocates containers/resources
```

Why YARN is needed:

- Multiple users share the same cluster.
- Jobs need memory and CPU.
- Without a resource manager, jobs can fight for resources.
- Cluster utilization must be controlled.

## HDFS Internal Working

### Main Components

```text
HDFS Cluster

Client
  |
  v
NameNode  -> Stores metadata
  |
  +-- DataNode 1 -> Stores actual blocks
  +-- DataNode 2 -> Stores actual blocks
  +-- DataNode 3 -> Stores actual blocks
```

### NameNode

The NameNode is the master service in HDFS.

It stores metadata.

Metadata means information about data, not the actual file content.

NameNode stores:

- File names.
- Directory structure.
- File permissions.
- File to block mapping.
- Block to DataNode mapping.

Analogy:

The NameNode is like a library catalog. It tells where the book is kept, but it is not the book itself.

### DataNode

DataNodes store the actual data blocks.

They:

- Store blocks on local disks.
- Send heartbeats to NameNode.
- Serve read requests to clients.
- Receive write requests from clients.
- Replicate blocks when instructed.

### Reading A File In HDFS

```text
Client wants to read file
    |
    v
Client asks NameNode for block locations
    |
    v
NameNode returns DataNode locations
    |
    v
Client reads blocks directly from DataNodes
```

Important:

The actual file data does not pass through the NameNode during normal reads. The NameNode only gives metadata.

### Heartbeat

Each DataNode sends a heartbeat to the NameNode.

The heartbeat means:

"I am alive and available."

Course note:

- DataNode sends heartbeat every few seconds.
- If NameNode does not receive heartbeat for a configured time, it marks the DataNode as dead.

Why heartbeat matters:

- Detects failed nodes.
- Triggers block re-replication.
- Keeps cluster metadata updated.

### What If A DataNode Fails?

If a DataNode fails:

- NameNode notices missing heartbeat.
- Blocks on that DataNode are considered unavailable.
- HDFS checks whether replicas exist on other DataNodes.
- If replication factor is below target, HDFS creates new replicas.

Example:

```text
Replication factor = 3

Block A exists on:
    DataNode 1
    DataNode 2
    DataNode 3

DataNode 2 fails

Block A still exists on:
    DataNode 1
    DataNode 3

HDFS creates one new copy on DataNode 4
```

### Data Locality

Data locality means processing the data on the same machine where the data is stored, or as close to it as possible.

Why this matters:

- Moving huge data over the network is expensive.
- It is usually faster to move the processing task to the data than to move the data to the processing task.
- Hadoop was designed around this principle.

Simple example:

```text
Data block is on DataNode 2
        |
        v
Run the processing task on DataNode 2 if resources are available
```

This was very important in classic Hadoop clusters because network bandwidth was limited compared with local disk access.

Modern cloud note:

With object storage like S3 or ADLS, compute and storage are often separated. Data locality is less direct than HDFS, but the same idea still matters as "avoid unnecessary data movement."

### What If NameNode Fails?

In old Hadoop versions, NameNode was a single point of failure.

Modern Hadoop supports High Availability.

Typical setup:

```text
Active NameNode
Standby NameNode
JournalNodes / shared edits
ZooKeeper for failover coordination
```

Important correction:

The Secondary NameNode is not a real backup NameNode. It mainly helps with checkpointing metadata. In modern production Hadoop, NameNode High Availability uses active and standby NameNodes.

This is a common interview trap.

## Hadoop Ecosystem Tools

Classic Hadoop ecosystem:

```text
Source Systems
    |
    v
Sqoop / Flume
    |
    v
HDFS
    |
    v
MapReduce / Pig / Hive / Spark
    |
    v
Hive / HBase / RDBMS
    |
    v
Reports / Applications
```

### Sqoop

Sqoop was used for moving data between relational databases and Hadoop.

Use case:

- Import MySQL table into HDFS.
- Export processed HDFS data back to RDBMS.

Example:

```text
MySQL table -> Sqoop import -> HDFS files
HDFS files  -> Sqoop export -> MySQL table
```

Modern alternatives:

- AWS Glue.
- Azure Data Factory.
- Google Cloud Dataflow.
- Airbyte.
- Fivetran.
- Debezium for CDC.

When to use:

- Legacy Hadoop environments.

When not to use:

- New cloud-native pipelines.
- Real-time CDC use cases.

### Pig

Pig is a scripting tool used to process and clean data on Hadoop.

It uses Pig Latin language.

It internally runs MapReduce jobs.

Today Pig is mostly legacy.

### Hive

Hive gives a SQL-like interface on Hadoop data.

Why Hive became popular:

- Analysts know SQL.
- Easier than writing MapReduce.
- Can query large files in HDFS.

Example Hive query:

```sql
SELECT country, COUNT(*) AS total_orders
FROM orders
GROUP BY country;
```

Old Hive used MapReduce as the execution engine. Modern Hive can use Tez or Spark.

### Oozie

Oozie is a workflow scheduler for Hadoop jobs.

Example workflow:

```text
Job M1 and Job M2 run in parallel
        |
        v
Job M3 runs after both finish
```

Modern alternatives:

- Apache Airflow.
- Azure Data Factory.
- AWS Step Functions.
- Databricks Workflows.
- Dagster.
- Prefect.

### HBase

HBase is a NoSQL database built on top of HDFS.

It is used when fast random read/write access is needed on huge data.

Use cases:

- Lookup user profile by user ID.
- Store time-series events.
- Serve application queries with low latency.

Modern alternatives:

- Cassandra.
- DynamoDB.
- Bigtable.
- Cosmos DB.

## Challenges With Hadoop

Hadoop solved the first generation of Big Data problems, but it also created pain points.

Common challenges:

- MapReduce is slow compared with modern engines.
- MapReduce code is verbose.
- Many ecosystem tools have separate learning curves.
- Cluster operations are complex.
- Small files cause NameNode pressure.
- On-prem clusters need hardware planning.
- Scaling takes effort in non-cloud environments.

Why Hadoop is less dominant today:

- Cloud object storage became cheap and scalable.
- Spark became the preferred compute engine.
- Managed services reduced cluster maintenance.
- Data lakehouse platforms simplified storage and processing.

Still important:

HDFS, YARN, Hive, partitioning, distributed storage, and distributed compute concepts are still useful in interviews and production.

## Apache Spark

### What Is Apache Spark?

Apache Spark is a general-purpose, in-memory, distributed compute engine.

Breaking that down:

- General-purpose: Can do batch, SQL, streaming, ML, and graph workloads.
- In-memory: Can keep intermediate data in RAM instead of writing to disk after every step.
- Distributed: Runs work across multiple machines.
- Compute engine: It processes data. It is not mainly a storage system.

### Why Spark Was Needed

MapReduce was slow and hard to write.

Spark improved this by:

- Providing easier APIs.
- Supporting Python, Scala, Java, and R.
- Keeping data in memory between operations.
- Supporting SQL, streaming, ML, and graph processing in one engine.

### Is Spark A Replacement For Hadoop?

Spark is not a full replacement for Hadoop.

Hadoop includes:

```text
Storage:             HDFS
Processing:          MapReduce
Resource Management: YARN
```

Spark mainly replaces MapReduce as the processing engine.

Spark still needs:

- Storage, such as HDFS, S3, ADLS, GCS, or local files.
- Resource manager, such as YARN, Kubernetes, Mesos, or Spark standalone.

Diagram:

```text
Storage Layer
HDFS / S3 / ADLS / GCS
        |
        v
Spark Processing Engine
        |
        v
Resource Manager
YARN / Kubernetes / Standalone
```

### Why Spark Is Faster Than MapReduce

Spark can be 10x to 100x faster for some workloads because:

- It avoids writing every intermediate result to disk.
- It uses memory more efficiently.
- It builds an execution plan before running.
- It supports optimized SQL execution through Catalyst.
- It uses Tungsten for memory and CPU efficiency.

Important:

Spark is not always faster automatically. Bad joins, data skew, too many small files, wrong partitioning, and bad cluster sizing can make Spark slow.

### Spark Languages

Developers can write Spark using:

- Scala.
- Python.
- Java.
- R.
- SQL.

Spark itself is written mainly in Scala.

Spark with Python is called PySpark.

Practical note:

- Data engineers often use PySpark because Python is easy and widely used.
- Scala can be faster for some advanced cases and is common in some older Spark shops.
- SQL is heavily used for transformations in Databricks, Spark SQL, and lakehouse pipelines.

## Cloud And Its Advantages

### On-Premise

On-premise means the company owns and manages its own data center or servers.

Responsibilities:

- Buy hardware.
- Install servers.
- Maintain network.
- Patch systems.
- Handle capacity planning.
- Manage disaster recovery.

Example:

If a company wants a 50-node Hadoop cluster on-premise, it must:

- Buy 50 physical servers.
- Arrange rack space or a data center.
- Set up networking.
- Arrange cooling equipment.
- Install Hadoop and related tools.
- Hire people to maintain hardware and software.
- Plan capacity before the business actually needs it.

This is slow and expensive because money is spent upfront.

### Cloud

Cloud means using computing resources from providers like AWS, Azure, or GCP.

Common cloud providers:

- AWS: Amazon Web Services.
- Azure: Microsoft Azure.
- GCP: Google Cloud Platform.

### Advantages Of Cloud

#### Scalability

Cloud systems can scale up or down based on demand.

Example:

- Use 2 workers during normal load.
- Use 20 workers during month-end processing.
- Scale back down after the job finishes.

#### CapEx Vs OpEx

CapEx means capital expenditure.

This is upfront spending, like buying servers.

OpEx means operational expenditure.

This is pay-as-you-use spending, like paying cloud bills monthly.

Cloud usually shifts cost from CapEx to OpEx.

#### Agility

On-prem clusters can take months to procure and set up.

Cloud clusters can be created in minutes.

This helps teams experiment faster.

#### Geo-Distribution

Cloud providers have data centers in many regions.

This helps reduce latency.

Example:

Indian users can be served from an Indian region instead of a US region.

#### Disaster Recovery

Cloud makes backup and disaster recovery easier.

Examples:

- Store copies in another region.
- Use multi-zone clusters.
- Replicate object storage.
- Automate infrastructure rebuilds.

#### Cost Effectiveness

Cloud can be cost effective when used properly.

But cloud can also become expensive if:

- Clusters are left running.
- Data is duplicated unnecessarily.
- Queries scan too much data.
- Wrong instance types are used.
- Data transfer costs are ignored.

Simple comparison:

| Area | On-Premise | Cloud |
|---|---|---|
| Hardware | Company buys servers | Provider manages infrastructure |
| Setup time | Weeks or months | Minutes or hours |
| Scaling | Buy and install more servers | Add/remove resources quickly |
| Cost model | Upfront CapEx | Pay-as-you-use OpEx |
| Maintenance | Company responsibility | Shared with cloud provider |
| Best for | Strict control, legacy setups | Agility, elasticity, managed services |

## Cloud Types

### Public Cloud

Public cloud is shared cloud infrastructure provided by AWS, Azure, or GCP.

Use when:

- Data is not extremely sensitive, or compliance allows it.
- You want managed services.
- You need fast scaling.

Examples:

- AWS.
- Azure.
- GCP.

### Private Cloud

Private cloud is cloud-like infrastructure dedicated to one organization.

Use when:

- Strict regulatory controls exist.
- Organization wants more control.
- Data cannot move to public cloud.

Example:

Banking or government systems may use private cloud for sensitive data.

### Hybrid Cloud

Hybrid cloud combines private and public cloud.

Example:

```text
Sensitive banking data      -> Private cloud
Generic analytics workload  -> Public cloud
```

Challenges:

- Network connectivity.
- Security policies.
- Data duplication.
- Governance across environments.
- Higher operational complexity.

## Database Vs Data Warehouse Vs Data Lake

This topic is extremely important in interviews.

### Database

A database usually stores day-to-day transactional data.

It is designed for OLTP.

OLTP means Online Transaction Processing.

OLTP systems handle many small, fast business transactions.

Examples:

- Create order.
- Update account balance.
- Check product inventory.
- Add customer address.

Characteristics:

- Stores current operational data.
- Mostly structured data.
- Strong constraints and transactions.
- Optimized for inserts, updates, deletes, and point lookups.
- Follows schema-on-write.

Schema-on-write means data is validated against schema before storing.

Example:

If `order_amount` must be numeric, the database rejects text like `abc`.

Common databases:

- MySQL.
- PostgreSQL.
- Oracle.
- SQL Server.

When to use:

- Application backend.
- Transactional systems.
- Strong consistency requirements.

When not to use:

- Petabyte-scale raw data storage.
- Large historical analytics.
- Cheap storage of logs, images, videos, and raw files.

### Data Warehouse

A data warehouse stores structured historical data for analysis.

It is designed for OLAP.

OLAP means Online Analytical Processing.

OLAP systems answer analytical questions.

Examples:

- Monthly revenue by region.
- Top selling products by quarter.
- Customer churn by segment.
- Average delivery delay by city.

Characteristics:

- Stores historical data.
- Mostly structured data.
- Data comes from multiple source systems.
- Optimized for analytical queries.
- Usually follows schema-on-write.
- Often uses dimensional modeling.

Examples:

- Teradata.
- Amazon Redshift.
- Snowflake.
- Google BigQuery.
- Azure Synapse Dedicated SQL Pool.

### Data Lake

A data lake stores raw data in its original format.

It can hold:

- Structured data.
- Semi-structured data.
- Unstructured data.

Examples:

- CSV.
- JSON.
- Parquet.
- Logs.
- Images.
- Videos.
- Sensor files.

Data lake usually follows schema-on-read.

Schema-on-read means data is stored first, and schema is applied later when reading or processing.

Example:

Store `employee.csv` as a file first. Later, when reading it with Spark, define columns like employee_id, name, department, salary.

Common data lake storage:

- HDFS.
- Amazon S3.
- Azure Data Lake Storage Gen2.
- Google Cloud Storage.

### ETL Vs ELT

#### ETL

ETL means Extract, Transform, Load.

```text
Source
  |
  v
Extract
  |
  v
Transform
  |
  v
Load into warehouse
```

Used commonly with data warehouses.

Problem:

You must decide transformations before loading. This can reduce flexibility.

#### ELT

ELT means Extract, Load, Transform.

```text
Source
  |
  v
Extract
  |
  v
Load raw data into lake
  |
  v
Transform when needed
```

Used commonly with data lakes and lakehouses.

Benefit:

Raw data remains available for future use cases.

### Comparison Table

| Feature | Database | Data Warehouse | Data Lake |
|---|---|---|---|
| Main purpose | Transactions | Analytics | Raw scalable storage |
| Processing type | OLTP | OLAP | ELT, analytics, ML |
| Data type | Structured | Mostly structured | Structured, semi-structured, unstructured |
| Schema | Schema-on-write | Schema-on-write | Schema-on-read |
| Cost | High for large history | Medium to high | Low storage cost |
| Example | MySQL | Redshift, Teradata | S3, ADLS, HDFS |
| Users | Applications | Analysts, BI teams | Data engineers, scientists, analysts |

## Lakehouse

The course introduces database, warehouse, and lake. In modern interviews, I should also know lakehouse.

A lakehouse combines data lake storage with warehouse-like features.

It tries to provide:

- Low-cost storage.
- Open file formats.
- ACID transactions.
- Schema enforcement.
- Time travel.
- Data quality.
- BI and ML support.

Examples:

- Delta Lake.
- Apache Iceberg.
- Apache Hudi.

Simple view:

```text
Data Lake + Warehouse Features = Lakehouse
```

Production example:

```text
Raw data in S3/ADLS
    |
    v
Delta/Iceberg tables
    |
    v
Spark/Trino/Databricks/Snowflake query engines
```

Interview tip:

If someone asks "Data lake vs data warehouse", mention lakehouse as the modern bridge.

## Big Data Engineering Flow

Course flow:

```text
Multiple Sources
      |
      v
Ingestion Framework
      |
      v
Data Lake Storage
      |
      v
Processing Framework
      |
      v
Serving Layer
      |
      v
Visualization / BI / Applications
```

### Data Sources

Sources can include:

- Relational databases.
- Data warehouses.
- APIs.
- Application logs.
- Kafka topics.
- IoT sensors.
- Social media feeds.
- Files from vendors.
- Third-party SaaS tools.

### Ingestion Layer

Ingestion means bringing data from source systems into the data platform.

Types:

- Batch ingestion: Scheduled load, like every hour or day.
- Streaming ingestion: Continuous event ingestion.
- CDC ingestion: Capture inserts, updates, and deletes from databases.

Tools:

- Sqoop for legacy Hadoop.
- Kafka for events.
- Debezium for CDC.
- Azure Data Factory.
- AWS Glue.
- AWS DMS.
- Airbyte.
- Fivetran.

### Storage Layer

Storage holds raw and processed data.

Common zones:

```text
Raw / Bronze
    |
    v
Cleaned / Silver
    |
    v
Aggregated / Gold
```

Bronze:

- Raw copy from source.
- Minimal changes.
- Used for replay and auditing.

Silver:

- Cleaned, deduplicated, standardized.
- Schema applied.

Gold:

- Business-ready data.
- Aggregated or modeled for BI, ML, or applications.

### Processing Layer

Processing transforms data.

Common operations:

- Filtering.
- Joining.
- Deduplication.
- Aggregation.
- Data type conversion.
- Data quality validation.
- Slowly changing dimension handling.
- Window calculations.

Tools:

- Spark.
- Flink.
- Hive.
- SQL engines.
- Databricks.
- Glue.
- Synapse.

### Serving Layer

Serving layer is where processed data is made available to consumers.

Common serving systems:

- Hive tables.
- Data warehouse tables.
- Delta tables.
- SQL database.
- NoSQL database.
- Search index.
- Feature store.

Which serving system to choose:

- BI reporting: Warehouse, Hive, Delta, BigQuery, Redshift, Snowflake.
- Fast application lookup: NoSQL like HBase, DynamoDB, Cosmos DB.
- Search use case: Elasticsearch/OpenSearch.
- ML features: Feature store or curated lakehouse tables.

## Data Pipeline On Hadoop

Classic on-prem Hadoop pipeline:

```text
MySQL / Oracle / RDBMS
        |
        v
Sqoop
        |
        v
HDFS
        |
        v
MapReduce / Spark
        |
        v
Hive
        |
        v
Tableau / Power BI
```

For custom UI requiring fast lookups:

```text
MySQL / Oracle / RDBMS
        |
        v
Sqoop
        |
        v
HDFS
        |
        v
Spark
        |
        v
HBase
        |
        v
Custom Application UI
```

Why Hive for BI:

- SQL interface.
- Good for reporting.
- Analysts can query tables.

Why HBase for custom UI:

- Fast random access.
- Better for point lookups.
- Useful when application needs quick retrieval by key.

## Data Pipeline On Cloud

Azure example from the course:

```text
Multiple Sources
        |
        v
Azure Data Factory
        |
        v
Azure Data Lake Storage Gen2
        |
        v
Azure Databricks / Synapse
        |
        v
Azure SQL / Cosmos DB
        |
        v
Power BI / Custom UI
```

AWS equivalent:

```text
Multiple Sources
        |
        v
AWS DMS / Glue / Kafka / Kinesis
        |
        v
Amazon S3
        |
        v
AWS Glue / EMR / Databricks / Athena / Spark
        |
        v
Redshift / DynamoDB / OpenSearch
        |
        v
QuickSight / Tableau / APIs
```

Modern lakehouse example:

```text
Application DB + Kafka + APIs
        |
        v
Ingestion: ADF / Glue / Kafka Connect
        |
        v
Bronze Delta Tables
        |
        v
Silver Delta Tables
        |
        v
Gold Delta Tables
        |
        +-- Power BI
        +-- ML Features
        +-- Reverse ETL
        +-- Data APIs
```

## Categories Of Computation

### Serverless Computing

Serverless means I do not manage fixed servers directly.

The cloud provider allocates resources behind the scenes.

Examples:

- Amazon Athena.
- Azure Synapse Serverless SQL.
- BigQuery.
- AWS Lambda for small event workloads.

Course-style cost example:

Amazon Athena charges based on the amount of data scanned.

If Athena charges about `$5` for 1 TB scanned:

```text
Query scans 1 TB    -> cost is about $5
Query scans 100 GB  -> cost is about $0.50
```

This is why file format, partitioning, and selecting only required columns matter so much in serverless query engines.

Advantages:

- No cluster management.
- Pay per query or usage.
- Good for occasional workloads.
- Easy to start.

Limitations:

- Less control over performance.
- Cold starts or queueing can happen.
- Expensive if queries scan huge data repeatedly.
- Not always ideal for strict low-latency workloads.

Good for:

- Ad hoc queries.
- Scheduled jobs that can tolerate some variability.
- Exploring data in object storage.

Not good for:

- Jobs needing guaranteed resources.
- Heavy repeated processing without cost controls.
- Very low-latency serving.

### Serverful Computing

Serverful means resources are dedicated or explicitly provisioned.

Examples:

- Amazon Redshift provisioned cluster.
- Azure Synapse Dedicated SQL Pool.
- EMR cluster.
- Databricks all-purpose cluster.
- Kubernetes Spark cluster.

Example:

With AWS Redshift, I turn on a dedicated cluster. Because the resources are reserved for me, performance is more predictable, but the cluster can cost a lot if it stays running.

Advantages:

- More control.
- More predictable performance.
- Good for steady workloads.
- Can tune cluster size and instance types.

Limitations:

- Costs money while running.
- Needs sizing and management.
- Idle clusters waste cost.

Good for:

- Production ETL jobs with SLAs.
- Ad hoc jobs that need immediate and predictable execution.
- Heavy transformations.
- Repeated BI workloads.
- Large Spark jobs.

Not good for:

- Rare one-off queries.
- Workloads that run once a month unless cluster auto-terminates.

## Code Examples

These examples are simple but interview useful.

### Example 1: SQL - Basic Aggregation

Problem:

Find total sales by product.

```sql
SELECT
    product_id,
    SUM(amount) AS total_sales
FROM orders
GROUP BY product_id
ORDER BY total_sales DESC;
```

Explanation:

- `SELECT product_id` keeps one output row per product.
- `SUM(amount)` calculates total revenue.
- `GROUP BY product_id` groups all order rows for the same product.
- `ORDER BY total_sales DESC` shows highest-selling products first.

Expected output:

```text
product_id | total_sales
-----------+------------
P101       | 120000
P205       |  85000
P333       |  54000
```

Common mistake:

Selecting a non-aggregated column without grouping it.

Wrong:

```sql
SELECT product_id, order_id, SUM(amount)
FROM orders
GROUP BY product_id;
```

Why wrong:

`order_id` has many values per product, so SQL does not know which order_id to show.

### Example 2: PySpark - Read CSV And Aggregate

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

spark = SparkSession.builder.appName("SalesAggregation").getOrCreate()

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/data/raw/orders.csv")
)

result_df = (
    orders_df
    .groupBy("product_id")
    .agg(spark_sum("amount").alias("total_sales"))
    .orderBy("total_sales", ascending=False)
)

result_df.show()
```

Explanation:

- `SparkSession` is the entry point for Spark.
- `.option("header", "true")` tells Spark the first row has column names.
- `.option("inferSchema", "true")` asks Spark to detect data types.
- `.groupBy("product_id")` groups rows by product.
- `.agg(...)` performs aggregation.
- `.alias("total_sales")` gives a readable output column name.
- `.show()` prints sample output.

Expected output:

```text
+----------+-----------+
|product_id|total_sales|
+----------+-----------+
|P101      |120000     |
|P205      |85000      |
|P333      |54000      |
+----------+-----------+
```

Common mistakes:

- Using `inferSchema` on huge production files because it causes extra scan.
- Forgetting that Spark is lazy. Transformations run only when an action like `.show()` or `.write()` happens.
- Reading many small CSV files without compaction.

Production improvement:

Define schema manually.

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
])

orders_df = (
    spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv("/data/raw/orders.csv")
)
```

Why this is better:

- Avoids schema inference cost.
- Prevents inconsistent schema between files.
- Makes pipeline behavior predictable.

### Example 3: PySpark - Bronze To Silver Cleaning

```python
from pyspark.sql.functions import col, to_timestamp

bronze_df = spark.read.json("/lake/bronze/orders/")

silver_df = (
    bronze_df
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .withColumn("order_ts", to_timestamp(col("order_time")))
    .dropDuplicates(["order_id"])
)

(
    silver_df.write
    .mode("overwrite")
    .parquet("/lake/silver/orders/")
)
```

Explanation:

- Reads raw JSON data from bronze zone.
- Removes rows without key identifiers.
- Converts string timestamp to proper timestamp.
- Removes duplicate orders based on `order_id`.
- Writes cleaned data to silver zone in Parquet format.

Why Parquet:

- Columnar format.
- Better compression.
- Faster analytical queries.
- Stores schema.

Expected result:

```text
/lake/silver/orders/
    part-00000-....
    part-00001-....
```

Common mistakes:

- Using overwrite without partition awareness and deleting more data than intended.
- Dropping duplicates without understanding late-arriving updates.
- Not storing bad records separately for debugging.

### Example 4: HDFS Commands

Create directory:

```bash
hdfs dfs -mkdir -p /user/data/orders
```

Upload file:

```bash
hdfs dfs -put orders.csv /user/data/orders/
```

List files:

```bash
hdfs dfs -ls /user/data/orders
```

Read first lines:

```bash
hdfs dfs -cat /user/data/orders/orders.csv | head
```

Expected output for list:

```text
-rw-r--r--   3 dataeng supergroup  102400 2026-07-26 /user/data/orders/orders.csv
```

Important:

The number `3` often represents replication factor.

Common mistake:

Treating HDFS like a normal local file system. HDFS is optimized for large files and streaming reads, not many tiny files and random updates.

### Example 5: Interview-Level Pipeline Design

Question:

Build a daily sales reporting pipeline from MySQL to a dashboard.

Possible design:

```text
MySQL orders table
      |
      v
Ingestion: ADF / Glue / Sqoop / CDC tool
      |
      v
Raw zone: S3 / ADLS / HDFS
      |
      v
Spark cleaning job
      |
      v
Curated sales table
      |
      v
Warehouse / Delta Gold table
      |
      v
Power BI / Tableau
```

Key design choices:

- Use incremental load instead of full load every day.
- Store raw data before transformation.
- Partition by date for efficient reads.
- Add data quality checks.
- Track pipeline run metadata.
- Monitor freshness and record counts.
- Make job idempotent.

Idempotent means running the same job again should not create duplicate or wrong data.

## Real-World Example

### E-Commerce Sale Analytics

Business question:

"How many iPhones were sold in the last 1 hour, and which region is selling fastest?"

Architecture:

```text
Website / Mobile App
        |
        v
Kafka Topic: order_events
        |
        v
Spark Structured Streaming
        |
        v
Delta Lake Silver Orders
        |
        v
Aggregated Gold Sales Table
        |
        v
Dashboard / Alerting
```

Why this design:

- Kafka handles high-velocity events.
- Spark processes events continuously.
- Delta Lake gives reliable storage and ACID transactions.
- Gold table serves dashboard quickly.

Possible production challenges:

- Duplicate order events.
- Late events.
- Payment status changes.
- Schema changes from application team.
- Dashboard query scanning too much data.
- Streaming job failure.

How to handle:

- Deduplicate by order_id and event_id.
- Use watermarking for late data.
- Store raw events.
- Use schema registry or schema validation.
- Partition by event_date.
- Monitor lag, throughput, and failures.

## Role Of A Data Engineer

A Data Engineer acts like the bridge between data owners and data consumers.

Data owners are teams or systems that generate data.

Examples:

- Application teams.
- Payment systems.
- CRM systems.
- Sensor systems.
- Third-party vendors.

Data consumers are people or systems that use processed data.

Examples:

- Business analysts.
- Data scientists.
- Machine learning models.
- Reporting teams.
- Product teams.
- Customer-facing applications.

Simple view:

```text
Data Owners
    |
    v
Data Engineer
    |
    |-- Ingest data
    |-- Store raw data
    |-- Clean and transform data
    |-- Create reliable pipelines
    |-- Serve usable data
    v
Data Consumers
```

Traditional approach:

```text
Multiple Sources
      |
      v
ETL Tools: Informatica / Talend
      |
      v
Data Warehouse
```

Modern approach:

```text
Multiple Sources
      |
      v
Ingestion: ADF / AWS Glue / Kafka / CDC
      |
      v
Data Lake
      |
      v
Spark / SQL Processing
      |
      v
Serving Layer
```

Why the modern approach became popular:

- Data warehouse storage was costly.
- Raw data can be stored cheaply in a data lake.
- Teams may not know all future transformations at ingestion time.
- ELT gives more flexibility.
- Spark and cloud platforms make large-scale processing easier.

In interviews, I should not describe a Data Engineer as only a pipeline developer.

A production Data Engineer is responsible for:

- Data ingestion.
- Data modeling.
- Data quality.
- Pipeline orchestration.
- Performance optimization.
- Cost optimization.
- Security and access control.
- Monitoring and alerting.
- Debugging production failures.
- Making data trustworthy for consumers.

## Best Practices

- Store raw data before transforming it.
- Use clear zones: bronze, silver, gold.
- Prefer columnar formats like Parquet for analytics.
- Avoid too many small files.
- Partition data based on query patterns.
- Use incremental processing where possible.
- Make pipelines idempotent.
- Add data quality checks.
- Track lineage from source to target.
- Monitor freshness, row counts, failures, and cost.
- Use managed cloud services where they reduce operational burden.
- Keep business logic version controlled.
- Document assumptions and data contracts.

## Common Mistakes

- Thinking Big Data means only huge volume.
- Ignoring velocity, variety, veracity, and value.
- Using a data warehouse as a dumping ground for raw data.
- Transforming data before storing raw copy when future use cases are unknown.
- Running full loads when incremental load is possible.
- Creating too many small files in data lake.
- Not handling duplicates.
- Not handling late-arriving data.
- Not monitoring pipeline freshness.
- Assuming Spark will automatically be fast.
- Over-partitioning tables.
- Under-partitioning large tables.
- Leaving cloud clusters running when idle.
- Confusing Secondary NameNode with standby NameNode.

## Performance Optimization

### Storage Performance

- Use Parquet or ORC for analytical workloads.
- Compress data with Snappy, ZSTD, or Gzip depending on use case.
- Avoid small files.
- Partition by columns commonly used in filters.
- Do not partition by high-cardinality columns like user_id unless there is a strong reason.
- Use bucketing or clustering where supported.

### Spark Performance

- Filter early to reduce data.
- Select only required columns.
- Avoid wide shuffles when possible.
- Broadcast small lookup tables.
- Repartition only when needed.
- Cache only reused DataFrames.
- Watch for data skew.
- Use explain plans.
- Tune executor memory and cores based on workload.

### Cloud Cost Performance

- Stop idle clusters.
- Use auto-scaling carefully.
- Use spot/preemptible instances for fault-tolerant jobs.
- Avoid scanning full lake for every query.
- Use lifecycle policies for old raw data.
- Monitor data transfer costs.

## Monitoring And Debugging

Production pipelines need monitoring.

Important metrics:

- Pipeline success/failure.
- Start time and end time.
- Runtime trend.
- Input row count.
- Output row count.
- Rejected row count.
- Data freshness.
- Data volume change.
- Spark executor failures.
- Shuffle size.
- Memory spill.
- Kafka consumer lag.
- Cloud cost.

Debugging approach:

1. Check whether source data arrived.
2. Check pipeline logs.
3. Compare input and output counts.
4. Check schema changes.
5. Check failed records.
6. Check cluster resource usage.
7. Check downstream table freshness.
8. Rerun idempotently after fixing.

## When To Use And When Not To Use

### Use Hadoop/HDFS When

- You are working in an existing Hadoop ecosystem.
- Data is already stored in HDFS.
- Organization runs on-prem Big Data clusters.

### Do Not Use Classic Hadoop For New Systems When

- Cloud object storage is available.
- Managed Spark/lakehouse services are available.
- Team does not want to operate clusters.

### Use Spark When

- Large-scale batch transformation is needed.
- Data is too large for single-machine Python.
- You need distributed joins and aggregations.
- You need one engine for SQL, batch, and streaming.

### Do Not Use Spark When

- Data is small enough for SQL or pandas.
- A simple database query solves the problem.
- Low-latency point lookup is needed.
- The overhead of cluster startup is bigger than the processing work.

### Use Data Warehouse When

- BI and reporting are the main goals.
- Data is structured and modeled.
- Users need fast SQL analytics.

### Use Data Lake When

- You need to store raw data cheaply.
- Data has many formats.
- Future use cases are unknown.
- ML and exploratory analysis need raw history.

## Interview Questions

### Beginner Questions

1. What is Big Data?
2. Explain the 5 V's of Big Data.
3. What is the difference between structured, semi-structured, and unstructured data?
4. What is vertical scaling?
5. What is horizontal scaling?
6. Why are distributed systems used in Big Data?
7. What are the core components of Hadoop?
8. What is HDFS?
9. What is MapReduce?
10. What is YARN?
11. What is Apache Spark?
12. Is Spark a replacement for Hadoop?
13. What is the difference between database and data warehouse?
14. What is a data lake?
15. What is ETL?
16. What is ELT?

### Intermediate Questions

1. Why is MapReduce slower than Spark?
2. How does HDFS store a large file?
3. What is the role of NameNode and DataNode?
4. What happens when a DataNode fails?
5. What is schema-on-write?
6. What is schema-on-read?
7. Why is raw data stored in a data lake?
8. When would you use Hive instead of HBase?
9. What is the difference between OLTP and OLAP?
10. What are the challenges of Hadoop?
11. Why is cloud preferred for modern Big Data platforms?
12. What is serverless computing?
13. What is serverful computing?
14. How would you design a batch data pipeline?
15. What is the bronze, silver, gold architecture?

### Senior Data Engineer Questions

1. How would you design a scalable data platform for a company moving from on-prem Hadoop to cloud?
2. How do you decide between data warehouse, data lake, and lakehouse?
3. How do you handle schema evolution in a data lake?
4. How do you prevent duplicate data in incremental pipelines?
5. How do you design idempotent pipelines?
6. How do you monitor data quality in production?
7. How do you optimize Spark jobs that process terabytes of data?
8. How do you handle small files in a data lake?
9. How do you manage cost in cloud data platforms?
10. How do you design a serving layer for BI and custom application lookup?
11. How do you troubleshoot data freshness issues?
12. How do you handle late-arriving events in streaming pipelines?
13. How do you migrate legacy Sqoop workflows to modern cloud ingestion?
14. What are the tradeoffs of serverless query engines?
15. How do you design disaster recovery for a data platform?

## Scenario-Based Questions

### Scenario 1: Spark Job Is Too Slow

Question:

Your Spark job is taking 5 hours instead of 30 minutes. How would you troubleshoot?

Answer approach:

- Check input data size and file count.
- Check whether there are too many small files.
- Check Spark UI stages and tasks.
- Look for shuffle-heavy operations.
- Check data skew.
- Check executor memory spills.
- Check joins and whether broadcast join is possible.
- Check partitioning.
- Check cluster size and executor configuration.
- Check whether unnecessary columns are being read.
- Check output write bottlenecks.

Follow-up questions:

- What is data skew?
- How do you fix data skew?
- When would you broadcast a table?
- What is shuffle?
- How do partitions affect Spark performance?

### Scenario 2: Dashboard Shows Wrong Sales Count

Question:

Business says the dashboard sales count is wrong. How do you debug?

Answer approach:

- Confirm exact metric definition.
- Check source data count.
- Check ingestion completeness.
- Check duplicate records.
- Check filters in transformation logic.
- Check timezone handling.
- Check late-arriving events.
- Check dashboard query.
- Compare bronze, silver, and gold counts.
- Reprocess affected date if pipeline is idempotent.

Follow-up questions:

- How do you design data reconciliation?
- How do you track lineage?
- How do you prevent duplicate orders?

### Scenario 3: Data Lake Has Too Many Small Files

Question:

Your data lake table has millions of small files and queries are slow. What do you do?

Answer approach:

- Compact small files into larger files.
- Tune streaming trigger or batch output partitions.
- Avoid writing one file per small batch.
- Use optimized writes if platform supports it.
- Partition carefully.
- Consider Delta/Iceberg/Hudi table optimization.

Follow-up questions:

- Why are small files bad?
- How does small file problem affect NameNode in HDFS?
- How does it affect object storage query engines?

### Scenario 4: Moving From Hadoop To Cloud

Question:

Your company wants to migrate on-prem Hadoop pipelines to AWS or Azure. What are the key steps?

Answer approach:

- Inventory current data, jobs, dependencies, SLAs.
- Identify HDFS data and Hive tables.
- Move raw and curated data to S3 or ADLS.
- Replace Sqoop with Glue, DMS, ADF, or CDC.
- Replace Oozie with Airflow, ADF, or Databricks Workflows.
- Run Spark jobs on EMR, Glue, Synapse, or Databricks.
- Validate outputs against old platform.
- Migrate consumers gradually.
- Add monitoring and cost controls.

Follow-up questions:

- How do you validate migration?
- How do you reduce downtime?
- How do you manage security and access control?

### Scenario 5: Need Real-Time Fraud Detection

Question:

A bank wants to detect suspicious transactions within seconds. Would you use batch or streaming?

Answer:

Streaming is better because decisions are time-sensitive.

Possible architecture:

```text
Transaction System
        |
        v
Kafka
        |
        v
Spark Streaming / Flink
        |
        v
Rules + ML Scoring
        |
        v
Alerts / Case Management / Block Transaction
```

Follow-up questions:

- How do you handle duplicate events?
- How do you handle late events?
- How do you guarantee exactly-once or effectively-once processing?
- What metrics would you monitor?

## Important Points To Remember

- Big Data is about scale, speed, variety, quality, and business value.
- Distributed systems solve scale by splitting work across machines.
- Hadoop has HDFS, MapReduce, and YARN.
- Spark replaces MapReduce for most modern processing, not full Hadoop.
- HDFS stores actual data in DataNodes and metadata in NameNode.
- HDFS commonly uses 128 MB blocks and replication factor 3.
- Hadoop follows data locality: process data close to where it is stored.
- Data warehouse is mainly for structured analytical data.
- Data lake stores raw data cheaply in many formats.
- ETL transforms before loading.
- ELT loads raw data first, then transforms later.
- Cloud gives scalability and agility but needs cost control.
- Serverless is easy to start but can have variable performance.
- Serverful gives more control but can waste money if idle.
- A Data Engineer is the bridge between data owners and data consumers.
- Production data engineering requires monitoring, quality checks, and debugging discipline.

## Interview Tips

- Do not define Big Data only as "large data". Mention the V's.
- When asked about Hadoop, clearly separate HDFS, MapReduce, and YARN.
- When asked about Spark vs Hadoop, say Spark replaces MapReduce, not HDFS or YARN.
- Mention real production issues: duplicates, late data, schema changes, small files, data skew, cost.
- Use diagrams when explaining pipeline architecture.
- For senior interviews, always discuss tradeoffs.
- Explain why a tool is chosen, not only what the tool does.
- Bring up monitoring and data quality without waiting for the interviewer to ask.

## Summary

This section gives the foundation of Big Data Engineering.

The core idea is simple:

Traditional systems struggle when data becomes too large, fast, varied, or messy. Big Data systems solve this by using distributed storage, distributed processing, and scalable architecture.

Hadoop introduced the first major ecosystem with HDFS, MapReduce, and YARN. Spark later became popular because it made distributed processing faster and easier. Cloud platforms made Big Data systems more agile and scalable by reducing the need to manage physical infrastructure.

A modern data engineer must understand not only tools, but also how data moves from source systems into storage, how it is transformed, how it is served, and how the full pipeline is monitored in production.

## Quick Revision

```text
Big Data = Volume + Variety + Velocity + Veracity + Value
```

```text
Monolithic system = one large machine = vertical scaling
Distributed system = many machines = horizontal scaling
```

```text
Hadoop
  |
  |-- HDFS = storage
  |-- MapReduce = processing
  |-- YARN = resource manager
```

```text
HDFS basics:
    Block size          -> commonly 128 MB
    Replication factor  -> commonly 3
    NameNode            -> metadata
    DataNode            -> actual data blocks
    Data locality       -> process near stored data
```

```text
Spark = general-purpose, in-memory, distributed compute engine
Spark replaces MapReduce, not full Hadoop
```

```text
Database      -> OLTP, current transactions, schema-on-write
Warehouse     -> OLAP, structured historical analytics, schema-on-write
Data Lake     -> raw scalable storage, all formats, schema-on-read
Lakehouse     -> data lake + warehouse features
```

```text
Modern Pipeline

Sources
   |
   v
Ingestion
   |
   v
Bronze Raw Data
   |
   v
Silver Clean Data
   |
   v
Gold Business Data
   |
   v
BI / ML / APIs / Apps
```

```text
Data Engineer = bridge between data owners and data consumers
Main work = ingest, store, process, serve, monitor, optimize
```
