# Data Architecture Notes

These notes explain data architecture in a simple, practical way. The goal is not to memorize heavy definitions, but to understand how data systems are designed so data can be collected, stored, processed, governed, and used properly.

## 1. What Is Data Architecture?

Data architecture is the overall design of how data flows through an organization.

In simple words:

> Data architecture is like the blueprint of the full data system.

If data modeling is about designing tables and relationships, data architecture is about designing the complete data environment around those tables.

It answers questions like:

- Where does data come from?
- Where should data be stored?
- How does data move from source systems to reports?
- Which tools are used for ingestion, processing, storage, and analytics?
- How do we secure sensitive data?
- How do we make sure the data is trusted?
- How do users access the data?

Good data architecture helps with:

- **Reliability**: Data pipelines run correctly and consistently.
- **Scalability**: The system can handle more users, more data, and more use cases.
- **Performance**: Reports, dashboards, and queries run efficiently.
- **Security**: Sensitive data is protected.
- **Governance**: Data ownership, quality, and definitions are clear.
- **Cost control**: Storage and compute are used wisely.
- **Business value**: Data is easy to use for decisions, analytics, and AI.

## 2. Data Architecture vs Data Modeling

Data architecture and data modeling are related, but they are not the same.

| Topic | Data Architecture | Data Modeling |
|---|---|---|
| Main focus | Full data system design | Structure of data tables |
| Scope | Sources, pipelines, storage, governance, access | Entities, columns, keys, relationships |
| Level | Broader | More detailed |
| Example question | How will data move from applications to warehouse? | What columns should be in `fact_sales`? |
| Output | Architecture diagram, platform design, data flow | ERD, schema, table design |

Simple example:

Data architecture decides:

```text
Application Database -> Data Pipeline -> Data Lake -> Data Warehouse -> BI Dashboard
```

Data modeling decides:

```text
fact_sales
dim_customer
dim_product
dim_date
```

## 3. Main Goals of Data Architecture

The main goal of data architecture is to make data usable, trusted, secure, and available.

A good data architecture should support:

- Daily reporting
- Business dashboards
- Advanced analytics
- Machine learning
- Data sharing
- Auditing
- Compliance
- Operational decision-making

It should also reduce confusion.

Without good architecture, companies usually face problems like:

- Same metric has different values in different reports.
- Pipelines fail frequently.
- Data is duplicated everywhere.
- Nobody knows which table is correct.
- Sensitive data is visible to the wrong users.
- Reports are slow.
- Data teams spend more time fixing issues than building value.

## 4. Basic Data Architecture Flow

A simple data architecture usually looks like this:

```text
Source Systems
      |
      v
Data Ingestion
      |
      v
Data Storage
      |
      v
Data Processing
      |
      v
Data Modeling
      |
      v
Data Serving
      |
      v
Reports, Dashboards, Analytics, AI
```

Example:

```text
E-commerce App
      |
      v
Kafka or Batch ETL
      |
      v
Data Lake
      |
      v
Spark Transformations
      |
      v
Data Warehouse
      |
      v
Power BI Dashboard
```

## 5. Source Systems

Source systems are the original systems where data is created.

Examples:

- CRM system
- ERP system
- E-commerce application
- Banking application
- Mobile app
- Website tracking system
- Payment gateway
- Inventory system
- Excel files
- Third-party APIs

Example source data:

```text
customers
orders
payments
products
shipments
web_clicks
support_tickets
```

Important questions:

- Who owns the source system?
- How often does the data change?
- Is the data complete?
- Is the source data reliable?
- Is the source schema stable?
- Is the data structured, semi-structured, or unstructured?

## 6. Types of Data

Data architecture must support different types of data.

### 6.1 Structured Data

Structured data has a fixed format.

Example:

```text
customer_id  customer_name  email
101          Asha           asha@example.com
102          Ravi           ravi@example.com
```

Common storage:

- Relational databases
- Data warehouses
- Tables

### 6.2 Semi-Structured Data

Semi-structured data has some structure, but it is flexible.

Examples:

- JSON
- XML
- Avro
- Parquet with nested fields

Example JSON:

```json
{
  "customer_id": 101,
  "name": "Asha",
  "address": {
    "city": "Pune",
    "state": "Maharashtra"
  }
}
```

Common storage:

- Data lake
- NoSQL database
- Object storage

### 6.3 Unstructured Data

Unstructured data does not have a fixed table format.

Examples:

- PDF documents
- Images
- Audio files
- Videos
- Emails
- Chat messages

Common storage:

- Object storage
- Document stores
- Search indexes

## 7. Data Ingestion

Data ingestion means bringing data from source systems into the data platform.

In simple words:

> Data ingestion is the entry point of data into the architecture.

Examples:

- Copying data from a database to a data lake
- Reading events from Kafka
- Loading CSV files from SFTP
- Pulling data from an API
- Streaming click events from a website

### 7.1 Batch Ingestion

Batch ingestion loads data at scheduled intervals.

Examples:

- Every night at 1 AM
- Every hour
- Every 15 minutes

Use batch ingestion when:

- Real-time data is not required.
- Data volume is large.
- Source system provides files or scheduled extracts.
- Reports are updated daily or hourly.

Example:

```text
Every night:
orders table -> extract file -> data lake
```

### 7.2 Streaming Ingestion

Streaming ingestion loads data continuously.

Examples:

- Website clicks
- Payment events
- IoT sensor readings
- Fraud detection events
- Real-time order tracking

Use streaming ingestion when:

- Data must be processed quickly.
- Business needs near real-time decisions.
- Events arrive continuously.

Example:

```text
Website click -> Kafka topic -> streaming job -> analytics table
```

### 7.3 Batch vs Streaming

| Feature | Batch | Streaming |
|---|---|---|
| Data arrival | Scheduled | Continuous |
| Latency | Higher | Lower |
| Complexity | Simpler | More complex |
| Cost | Usually lower | Can be higher |
| Example | Daily sales report | Real-time fraud alert |

## 8. ETL and ELT

ETL and ELT are two common data processing patterns.

### 8.1 ETL

ETL means:

```text
Extract -> Transform -> Load
```

In ETL, data is transformed before loading into the final storage.

Example:

```text
Source database
      |
      v
ETL tool transforms data
      |
      v
Data warehouse
```

Use ETL when:

- Data warehouse should only receive clean data.
- Transformation happens outside the warehouse.
- Compliance requires filtering before storage.

### 8.2 ELT

ELT means:

```text
Extract -> Load -> Transform
```

In ELT, raw data is loaded first, then transformed inside the warehouse or lakehouse.

Example:

```text
Source database
      |
      v
Raw data loaded into data lake
      |
      v
Transform into clean analytics tables
```

Use ELT when:

- Storage is cheap.
- Compute is scalable.
- Raw data should be kept for replay.
- Transformations are done using SQL or Spark.

### 8.3 ETL vs ELT

| Feature | ETL | ELT |
|---|---|---|
| Transform happens | Before loading | After loading |
| Raw data stored | Usually no | Usually yes |
| Common in | Traditional warehouses | Cloud data platforms |
| Flexibility | Lower | Higher |
| Example tool style | Informatica-style ETL | dbt or Spark on lakehouse |

## 9. Data Storage Layers

Data architecture usually has multiple storage layers.

Common layers:

```text
Raw Layer -> Clean Layer -> Curated Layer -> Serving Layer
```

These layers help separate different levels of data quality and business readiness.

## 10. Data Lake

A data lake stores large amounts of raw and semi-processed data.

It can store:

- Structured data
- Semi-structured data
- Unstructured data
- Files
- Logs
- Events

Common data lake storage:

- Amazon S3
- Azure Data Lake Storage
- Google Cloud Storage
- HDFS

Example:

```text
/raw/orders/2026/10/05/orders.csv
/raw/clickstream/2026/10/05/events.json
/raw/payments/2026/10/05/payments.parquet
```

Benefits:

- Stores data cheaply
- Keeps raw history
- Supports replay
- Handles many file types

Challenges:

- Can become messy without governance
- Users may not know which data is trusted
- Poor naming and folder design can create confusion

## 11. Data Warehouse

A data warehouse stores structured, cleaned, business-ready data for reporting and analytics.

Examples:

- Snowflake
- BigQuery
- Redshift
- Azure Synapse
- Teradata
- Oracle Data Warehouse

Common warehouse tables:

```text
fact_sales
dim_customer
dim_product
sales_monthly_summary
customer_retention_kpi
```

Use a data warehouse when:

- Business users need reliable reporting.
- Data should be modeled clearly.
- Queries should be fast and consistent.
- Metrics and KPIs must be trusted.

## 12. Data Lakehouse

A data lakehouse combines ideas from a data lake and a data warehouse.

In simple words:

> A lakehouse tries to keep the flexibility of a data lake and the reliability of a warehouse.

Common technologies:

- Delta Lake
- Apache Iceberg
- Apache Hudi
- Databricks

Lakehouse features:

- Stores data in object storage
- Supports table formats
- Supports schema evolution
- Supports ACID transactions
- Supports batch and streaming
- Supports BI, analytics, and machine learning

Example:

```text
Raw files in data lake
      |
      v
Delta tables
      |
      v
Clean and curated analytics tables
```

## 13. Database vs Data Warehouse vs Data Lake

| Feature | Database | Data Warehouse | Data Lake |
|---|---|---|---|
| Main use | Applications and transactions | Analytics and reporting | Store raw and large data |
| Data type | Mostly structured | Structured | Structured, semi-structured, unstructured |
| Users | Applications | Analysts and BI users | Engineers, scientists, analysts |
| Design | Normalized | Dimensional or analytical | File and table based |
| Example | MySQL, PostgreSQL | Snowflake, BigQuery | S3, ADLS |

Simple example:

```text
Application database:
Used when a customer places an order.

Data lake:
Stores raw order files, logs, and events.

Data warehouse:
Stores clean sales tables for dashboards.
```

## 14. Medallion Architecture

Medallion architecture is a popular lakehouse pattern.

It usually has three layers:

```text
Bronze -> Silver -> Gold
```

### 14.1 Bronze Layer

Bronze stores raw data as received from source systems.

Characteristics:

- Minimal changes
- Mostly append-only
- Useful for audit
- Useful for replay
- May contain duplicates or invalid records

Example:

```text
bronze_orders_raw
bronze_customers_raw
bronze_clickstream_raw
```

### 14.2 Silver Layer

Silver stores cleaned and standardized data.

Common activities:

- Remove duplicates
- Standardize column names
- Fix data types
- Apply basic quality checks
- Join related source data
- Handle late-arriving records

Example:

```text
silver_orders
silver_customers
silver_products
```

### 14.3 Gold Layer

Gold stores business-ready data.

This layer is used for:

- Reporting
- Dashboards
- KPIs
- Data marts
- Machine learning features

Example:

```text
gold_fact_sales
gold_dim_customer
gold_sales_monthly_summary
```

Diagram:

```text
Source Systems
      |
      v
Bronze
Raw data
      |
      v
Silver
Clean data
      |
      v
Gold
Business-ready data
      |
      v
Reports and analytics
```

## 15. Data Pipeline

A data pipeline is a set of steps that moves and transforms data.

Example:

```text
Extract orders from source
      |
      v
Load into bronze
      |
      v
Clean into silver
      |
      v
Create fact_sales in gold
      |
      v
Refresh dashboard
```

Pipeline tasks may include:

- Extracting data
- Validating data
- Cleaning data
- Joining data
- Aggregating data
- Loading target tables
- Sending alerts

Good pipeline design should include:

- Error handling
- Logging
- Monitoring
- Retry logic
- Data quality checks
- Clear ownership

## 16. Orchestration

Orchestration means controlling when and how data pipelines run.

In simple words:

> Orchestration is like a scheduler and controller for data workflows.

Examples of orchestration tools:

- Apache Airflow
- Azure Data Factory
- Dagster
- Prefect
- Databricks Workflows

Example workflow:

```text
1. Load customers
2. Load orders
3. Load payments
4. Run data quality checks
5. Build fact_sales
6. Refresh dashboard dataset
```

Why orchestration matters:

- Pipelines run in the correct order.
- Failed jobs can be retried.
- Dependencies are clear.
- Teams can monitor pipeline health.

## 17. Data Quality

Data quality means the data is fit for use.

Common data quality checks:

- Completeness
- Accuracy
- Uniqueness
- Validity
- Consistency
- Timeliness

Examples:

```text
customer_id should not be null.
order_id should be unique.
order_date should not be in the future.
sales_amount should not be negative unless it is a refund.
country_code should exist in the reference table.
```

Data quality should be checked at multiple stages:

```text
Source -> Bronze -> Silver -> Gold -> Report
```

If quality checks fail, the system should:

- Log the issue
- Alert the owner
- Quarantine bad records if needed
- Avoid publishing wrong business numbers

## 18. Data Governance

Data governance means managing data properly across the organization.

It includes:

- Data ownership
- Data definitions
- Data quality rules
- Access control
- Data lineage
- Metadata
- Compliance
- Retention rules

Simple example:

If the business asks:

> What does active customer mean?

Data governance should provide one trusted definition.

Example definition:

```text
Active customer:
A customer who placed at least one completed order in the last 90 days.
```

Good governance avoids confusion and builds trust.

## 19. Metadata

Metadata means data about data.

Examples:

- Table name
- Column name
- Data type
- Table owner
- Description
- Refresh frequency
- Source system
- Last updated time
- Sensitivity level

Example:

```text
Table: gold_fact_sales
Owner: Sales Analytics Team
Refresh: Daily at 6 AM
Source: orders, order_items, payments
Description: Stores one row per sold order item
```

Metadata helps users understand:

- What the data means
- Where the data comes from
- Whether the data is safe to use
- Who to contact for questions

## 20. Data Catalog

A data catalog is a searchable inventory of data assets.

It helps users find and understand data.

Common data catalog features:

- Search tables
- View column descriptions
- See owners
- See data lineage
- See sample data
- See quality scores
- See access rules

Example tools:

- Collibra
- Alation
- Microsoft Purview
- Atlan
- DataHub
- OpenMetadata

Simple example:

An analyst searches for "sales revenue" and finds:

```text
gold_fact_sales
gold_sales_monthly_summary
sales_revenue_dashboard_dataset
```

## 21. Data Lineage

Data lineage shows where data came from and how it changed.

In simple words:

> Data lineage is the journey of data from source to final report.

Example:

```text
orders table
      |
      v
bronze_orders_raw
      |
      v
silver_orders
      |
      v
gold_fact_sales
      |
      v
Sales Dashboard
```

Why lineage matters:

- Helps debug wrong numbers
- Shows impact of source changes
- Helps with compliance
- Builds trust in reports

Example question:

> If `order_status` changes in the source, which dashboards will be affected?

Lineage helps answer this.

## 22. Data Security

Data security protects data from unauthorized access and misuse.

Common security practices:

- Authentication
- Authorization
- Role-based access control
- Encryption at rest
- Encryption in transit
- Masking sensitive data
- Auditing access
- Network security

Example:

```text
Only HR users can see employee salary.
Analysts can see employee department, but not salary.
```

Sensitive data examples:

- Email
- Phone number
- Address
- Bank account number
- Credit card number
- Salary
- Health records

Good architecture should apply security from the beginning, not after everything is built.

## 23. Data Privacy and Compliance

Data privacy is about protecting personal information.

Compliance means following laws, regulations, and company policies.

Examples of privacy rules:

- Do not expose personal data to unauthorized users.
- Keep personal data only as long as needed.
- Allow deletion or correction when required.
- Track who accessed sensitive data.
- Mask or tokenize sensitive columns.

Example:

```text
email: asha@example.com
masked email: a***@example.com
```

Important concepts:

- PII: Personally Identifiable Information
- Data retention
- Consent
- Right to delete
- Audit logs
- Least privilege access

## 24. Master Data Management

Master Data Management, or MDM, manages important business entities consistently.

Common master data:

- Customer
- Product
- Supplier
- Employee
- Location

Problem example:

```text
CRM system: Asha Sharma
Billing system: A. Sharma
Support system: Asha S.
```

These may refer to the same customer.

MDM tries to create one trusted customer record:

```text
golden_customer_id: C1001
customer_name: Asha Sharma
email: asha@example.com
phone: 9876543210
```

Benefits:

- Less duplication
- Better reporting
- Better customer view
- Consistent business definitions

## 25. Reference Data

Reference data is a controlled list of values used across systems.

Examples:

- Country codes
- Currency codes
- Product categories
- Order status values
- Payment methods
- Region codes

Example:

```text
order_status
------------
PENDING
COMPLETED
CANCELLED
REFUNDED
```

Why reference data matters:

- Keeps values consistent
- Reduces spelling differences
- Improves reporting
- Supports validation rules

Bad example:

```text
USA
U.S.A
United States
US
```

Better:

```text
country_code = US
country_name = United States
```

## 26. Data Mart

A data mart is a smaller, subject-specific area of data.

Examples:

- Sales data mart
- Finance data mart
- Marketing data mart
- HR data mart
- Customer data mart

Example:

```text
Enterprise Data Warehouse
      |
      v
Sales Data Mart
Finance Data Mart
Marketing Data Mart
```

Use a data mart when:

- A department needs focused data.
- Users need simple tables.
- Performance should be optimized for a specific business area.

## 27. Serving Layer

The serving layer is where users and applications consume data.

Examples:

- BI dashboards
- Semantic layer
- APIs
- Machine learning feature store
- Excel reports
- Embedded analytics

Common users:

- Business analysts
- Data analysts
- Data scientists
- Product managers
- Executives
- Applications

Example:

```text
Gold tables -> Semantic model -> Power BI dashboard
```

Good serving layer design should provide:

- Clear metrics
- Fast queries
- Secure access
- Consistent definitions
- Easy user experience

## 28. Semantic Layer

A semantic layer defines business-friendly metrics and dimensions.

In simple words:

> A semantic layer translates technical tables into business language.

Example:

Technical table:

```text
gold_fact_sales.sales_amount
```

Business metric:

```text
Revenue
```

The semantic layer may define:

- Revenue
- Gross profit
- Active customers
- Churn rate
- Average order value
- Region
- Product category

Why it matters:

- Same KPI logic is reused.
- Business users do not need to understand all database details.
- Reports become more consistent.

## 29. Cloud Data Architecture

Modern data architecture is often built in the cloud.

Common cloud services:

| Need | AWS Example | Azure Example | Google Cloud Example |
|---|---|---|---|
| Object storage | S3 | ADLS | Cloud Storage |
| Data warehouse | Redshift | Synapse | BigQuery |
| Processing | Glue, EMR | Databricks, Synapse Spark | Dataproc, Dataflow |
| Streaming | Kinesis, MSK | Event Hubs | Pub/Sub |
| Orchestration | Step Functions, MWAA | Data Factory | Cloud Composer |
| Catalog | Glue Data Catalog | Purview | Data Catalog |

Cloud architecture benefits:

- Scales up and down
- Less hardware management
- Many managed services
- Flexible storage and compute

Cloud architecture challenges:

- Cost can grow quickly
- Security must be designed carefully
- Too many services can create complexity
- Teams need good monitoring

## 30. Modern Data Stack

Modern data stack means a set of cloud-friendly tools used to build data platforms.

Example stack:

```text
Source systems
      |
      v
Fivetran or Airbyte
      |
      v
Snowflake or BigQuery
      |
      v
dbt
      |
      v
Looker or Power BI
```

Common tool categories:

- Ingestion
- Storage
- Transformation
- Orchestration
- Catalog
- Quality
- BI
- Monitoring

The exact tools are less important than the architecture principles.

Good architecture should still answer:

- Where is the raw data?
- Where is the trusted data?
- Who owns the data?
- How is data secured?
- How are failures handled?
- How are costs controlled?

## 31. Real-Time Data Architecture

Real-time architecture processes data quickly after it is created.

Example use cases:

- Fraud detection
- Live order tracking
- Stock market alerts
- Real-time personalization
- IoT monitoring
- Payment monitoring

Simple flow:

```text
Event Source
      |
      v
Kafka Topic
      |
      v
Stream Processing
      |
      v
Real-Time Store
      |
      v
Alert or Dashboard
```

Important design points:

- Event ordering
- Duplicate events
- Late events
- Exactly-once or at-least-once processing
- Low latency
- Monitoring

Real-time systems are powerful, but they are usually more complex than batch systems.

Use real-time only when the business really needs it.

## 32. Lambda and Kappa Architecture

### 32.1 Lambda Architecture

Lambda architecture uses both batch and streaming layers.

Example:

```text
Source Events
      |
      +--> Batch Layer
      |
      +--> Speed Layer
      |
      v
Serving Layer
```

Use it when:

- You need both historical accuracy and real-time views.
- Batch processing corrects or reprocesses old data.
- Streaming gives quick but possibly temporary results.

Challenge:

- Same logic may need to be maintained in two places.

### 32.2 Kappa Architecture

Kappa architecture uses streaming as the main processing pattern.

Example:

```text
Source Events -> Stream Processing -> Serving Tables
```

Use it when:

- Most data arrives as events.
- Reprocessing can happen by replaying events.
- The team wants a simpler streaming-first architecture.

Challenge:

- Streaming skills and tooling must be strong.

## 33. Data Mesh

Data mesh is an organizational approach to data architecture.

In data mesh, different business domains own their data products.

Examples of domains:

- Sales
- Marketing
- Finance
- Customer Support
- Supply Chain

Main ideas:

- Domain ownership
- Data as a product
- Self-service data platform
- Federated governance

Simple example:

```text
Sales team owns sales data product.
Finance team owns finance data product.
Marketing team owns campaign data product.
```

Data mesh is useful for large organizations where one central data team cannot manage everything alone.

But it needs strong standards, governance, and platform support.

## 34. Data Product

A data product is a trusted, reusable data asset built for users.

It should have:

- Clear owner
- Clear purpose
- Documented schema
- Quality checks
- Access rules
- Refresh schedule
- Support contact

Example:

```text
Data product: Customer 360
Owner: Customer Analytics Team
Refresh: Daily
Users: Marketing, Support, Product
Purpose: Provide a trusted full view of each customer
```

A table is not automatically a data product.

A data product should be reliable, documented, and useful.

## 35. Example End-to-End Data Architecture

Let us design a simple e-commerce data architecture.

Business needs:

- Daily sales dashboard
- Customer behavior analysis
- Product performance reports
- Fraud monitoring
- Marketing campaign analysis

Sources:

```text
E-commerce database
Payment gateway
Website clickstream
CRM system
Marketing platform
```

Architecture:

```text
Source Systems
      |
      v
Ingestion Layer
Batch loads and streaming events
      |
      v
Bronze Layer
Raw data
      |
      v
Silver Layer
Cleaned and standardized data
      |
      v
Gold Layer
Business-ready facts, dimensions, and summaries
      |
      v
Serving Layer
Dashboards, reports, ML features, APIs
```

Gold tables:

```text
gold_fact_sales
gold_dim_customer
gold_dim_product
gold_dim_date
gold_customer_360
gold_sales_daily_summary
```

Example dashboard questions:

- What is revenue by month?
- Which products are selling most?
- Which customers are most valuable?
- Which payment failures are increasing?
- Which campaigns created the most orders?

## 36. Common Data Architecture Mistakes

### Building without business requirements

If the architecture does not support business questions, it becomes a technical exercise only.

Better:

Start with business use cases, then design the platform.

### No clear ownership

If nobody owns a table or pipeline, nobody fixes it when it breaks.

Better:

Every important data asset should have an owner.

### Keeping everything in raw form only

Raw data is useful, but business users need clean and trusted data.

Better:

Use layers like bronze, silver, and gold.

### No data quality checks

Without quality checks, bad data silently reaches dashboards.

Better:

Validate important columns, counts, uniqueness, and business rules.

### Too much tool complexity

Using many tools without clear purpose makes the architecture hard to maintain.

Better:

Use tools only when they solve a real problem.

### Ignoring security

Security added late is harder to fix.

Better:

Design access control, masking, encryption, and auditing from the beginning.

### No cost monitoring

Cloud platforms can become expensive if compute and storage are not managed.

Better:

Monitor usage, optimize queries, archive old data, and choose the right storage format.

## 37. Good Data Architecture Principles

Good data architecture should be:

- Simple enough to understand
- Scalable enough to grow
- Secure by design
- Reliable in production
- Cost-aware
- Well documented
- Easy to monitor
- Flexible for future changes
- Focused on business value

Practical rules:

- Keep raw data when possible.
- Create trusted curated layers.
- Use clear naming standards.
- Track data lineage.
- Add data quality checks.
- Define ownership.
- Protect sensitive data.
- Avoid unnecessary complexity.

## 38. Quick Revision

Data architecture is the blueprint of the data platform.

It covers:

- Source systems
- Ingestion
- Storage
- Processing
- Modeling
- Serving
- Governance
- Security
- Quality
- Monitoring

Important concepts:

- Data lake stores raw and flexible data.
- Data warehouse stores clean analytics data.
- Lakehouse combines lake and warehouse ideas.
- ETL transforms before loading.
- ELT loads first and transforms later.
- Batch processes data on a schedule.
- Streaming processes data continuously.
- Bronze stores raw data.
- Silver stores cleaned data.
- Gold stores business-ready data.
- Data catalog helps users find data.
- Lineage shows the journey of data.
- Governance creates trust and clarity.
- Security protects sensitive information.

## 39. Simple Interview-Style Answers

### What is data architecture?

Data architecture is the design of how data is collected, stored, processed, governed, secured, and used across an organization.

### Why is data architecture important?

It helps make data reliable, scalable, secure, and useful for reporting, analytics, and business decisions.

### What is the difference between data architecture and data modeling?

Data architecture focuses on the full data system, including sources, pipelines, storage, governance, and access. Data modeling focuses on designing tables, columns, keys, and relationships.

### What is a data lake?

A data lake stores large amounts of raw data in different formats, such as files, logs, JSON, images, and tables.

### What is a data warehouse?

A data warehouse stores cleaned and structured data mainly for reporting, dashboards, and analytics.

### What is a lakehouse?

A lakehouse combines the flexibility of a data lake with the reliability and table features of a data warehouse.

### What is ETL?

ETL means Extract, Transform, Load. Data is transformed before it is loaded into the final target system.

### What is ELT?

ELT means Extract, Load, Transform. Data is loaded first, then transformed inside the warehouse or lakehouse.

### What is batch processing?

Batch processing loads or processes data at scheduled times, such as hourly or daily.

### What is streaming?

Streaming processes data continuously as events arrive, usually for near real-time use cases.

### What is data governance?

Data governance is the management of data ownership, definitions, quality, access, lineage, and compliance.

### What is data lineage?

Data lineage shows where data came from, how it changed, and where it is used.

### What is a data catalog?

A data catalog is a searchable inventory of data assets that helps users find, understand, and trust data.

### What is a data product?

A data product is a reliable, documented, reusable data asset with a clear owner, purpose, quality checks, and access rules.

### What is medallion architecture?

Medallion architecture is a data lakehouse pattern with bronze, silver, and gold layers. Bronze is raw, silver is cleaned, and gold is business-ready.

### What is a semantic layer?

A semantic layer defines business-friendly metrics and dimensions so reports use consistent KPI logic.

### What is the main job of a data architect?

A data architect designs the structure, flow, storage, security, governance, and usage of data systems so the organization can use data effectively.
