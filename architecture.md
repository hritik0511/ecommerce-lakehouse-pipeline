# Architecture — E-Commerce Lakehouse Data Engineering Platform

## 1. Architecture Overview

The platform uses a Medallion Architecture to transform historical and newly generated e-commerce data into analytics-ready datasets.

**Data flow:**

1. Historical Olist CSV files and synthetic batch files are stored in AWS S3.
2. Azure Databricks reads the files and creates Bronze Delta tables.
3. PySpark transformations clean, validate, and process the data into Silver Delta tables.
4. Gold tables organize the data into facts and dimensions for analysis.
5. Databricks SQL queries the Gold layer to produce business metrics.
6. Databricks Workflows orchestrate the notebook execution.

Azure Databricks is the processing workspace in this project; AWS S3 is the object storage location.

## 2. High-Level Architecture

```text
                 DATA SOURCES
        +---------------------------+
        |                           |
        |  Historical Olist CSVs    |
        |  Python Batch Simulator   |
        |                           |
        +-------------+-------------+
                      |
                      v
              AWS S3 - RAW ZONE
        +---------------------------+
        | Historical source files   |
        | Incoming synthetic batches|
        +-------------+-------------+
                      |
                      v
          AZURE DATABRICKS / PYSPARK
                      |
                      v
        +---------------------------+
        |       BRONZE LAYER        |
        |                           |
        | Raw Delta tables          |
        | Ingestion metadata        |
        | Source file tracking      |
        +-------------+-------------+
                      |
                      v
        +---------------------------+
        |       SILVER LAYER        |
        |                           |
        | Schema enforcement        |
        | Data cleaning             |
        | Deduplication             |
        | Data-quality checks       |
        | Invalid-record handling   |
        | Incremental processing    |
        | CDC and SCD Type 2        |
        +-------------+-------------+
                      |
                      v
        +---------------------------+
        |        GOLD LAYER         |
        |                           |
        | Fact tables               |
        | Dimension tables          |
        | Business aggregates       |
        +-------------+-------------+
                      |
                      v
        +---------------------------+
        |      DATABRICKS SQL       |
        |                           |
        | Sales and revenue         |
        | Customer analysis         |
        | Seller performance        |
        | Payment analysis          |
        | Delivery metrics          |
        +---------------------------+

          Databricks Workflows
       orchestrates notebook execution
```

## 3. Storage Organization

The existing S3 bucket is:

```text
s3://ecommerce-lakehouse-pipeline-dev/
```

The intended logical organization is:

```text
ecommerce-lakehouse-pipeline-dev/
|
|-- raw/
|   |-- Olist historical CSV files
|
|-- incoming/
|   |-- customers/
|   |-- orders/
|   |-- payments/
|   |-- order_items/
|
|-- lakehouse/
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|
|-- checkpoints/
```

This is the proposed organization. Existing bucket contents and Databricks storage configuration must be checked before creating or changing prefixes.

The Delta table storage location and checkpoint locations must be writable by the Databricks identity used by the pipeline. Do not expose AWS credentials, secret keys, account identifiers, or confidential information in GitHub.

## 4. Medallion Layers

### 4.1 Bronze Layer

**Purpose:** Store source data in Delta format with minimal transformation.

Main activities:

- Read historical CSV files from S3.
- Read newly arriving simulator batch files.
- Preserve source columns.
- Add ingestion metadata such as ingestion timestamp, source filename, and batch ID where available.
- Write Bronze Delta tables.
- Track which source files or batches have been processed.

Bronze should preserve the source information. Business cleaning and complex transformations belong in Silver.

### 4.2 Silver Layer

**Purpose:** Create clean, standardized, and validated datasets.

Main activities:

- Apply explicit data types and schemas.
- Standardize values and formats.
- Handle null values according to business rules.
- Identify and remove duplicates using appropriate business keys.
- Validate business rules and relationships between datasets.
- Separate invalid records for investigation.
- Process new records incrementally.
- Apply simulator-generated CDC events.
- Maintain historical customer and seller changes using SCD Type 2.

CDC and SCD Type 2 are related but different:

- **CDC** identifies and applies inserts, updates, and deletes from incoming change events.
- **SCD Type 2** preserves the history of selected dimension attributes by expiring the previous version and inserting a new version.

The simulator supplies explicit change events. The historical Olist files themselves are not assumed to contain CDC history.

### 4.3 Gold Layer

**Purpose:** Provide business-ready tables for analytical queries.

Proposed dimension tables:

| Table | Grain |
|---|---|
| `dim_customer` | One row per customer version |
| `dim_seller` | One row per seller version |
| `dim_product` | One row per product |
| `dim_date` | One row per calendar date |
| `dim_geography` | One row per normalized location |
| `dim_payment_type` | One row per payment type |

Proposed fact tables:

| Table | Grain |
|---|---|
| `fact_orders` | One row per order |
| `fact_order_items` | One row per order item |
| `fact_payments` | One row per payment installment |
| `fact_reviews` | One row per review |

The grain of each fact table must be maintained when joining or aggregating data. For example, joining order items directly to multiple payment rows can multiply values. Aggregations must account for these different grains.

## 5. Synthetic Batch Data Flow

A Python script will generate small, controlled batches of synthetic e-commerce records.

Example event types:

| Event | Example |
|---|---|
| INSERT | New order |
| INSERT | New payment |
| UPDATE | Customer address change |
| UPDATE | Order status change |
| DELETE | Explicit customer or seller deletion event |
| DUPLICATE | Repeated event |
| INVALID | Missing required identifier or invalid amount |

Each generated event should contain useful metadata:

- `event_id`
- `event_type`
- `entity_type`
- `entity_id`
- `event_timestamp`
- `batch_id`
- `source_system`
- `schema_version`

Entity-specific business fields will remain normal columns in the generated files. The simulator should also maintain a sequence or version for changes to an entity so that repeated and out-of-order events can be handled deliberately.

The simulator writes files to the S3 incoming area. Databricks then processes those files through the Bronze and Silver layers. The simulator does not write directly to Delta tables.

## 6. Incremental Processing and Reliability

The pipeline should process new files and events without repeatedly loading all historical data.

The implementation will use an incremental file-ingestion approach supported by Databricks. Auto Loader can be used for discovering new files, with `availableNow` processing for scheduled batch runs.

Important reliability considerations:

- Track source files and batch IDs.
- Use unique event IDs and business keys for deduplication.
- Use Delta Lake `MERGE` where appropriate for insert and update operations.
- Handle delete events explicitly.
- Make notebook execution safe to retry.
- Avoid processing the same event more than once.
- Store ingestion checkpoints in a durable, writable location.

A successful notebook run alone does not prove idempotency. We will validate it by processing the same batch again and checking that the resulting business records remain correct.

## 7. Data Quality and Monitoring

Data-quality checks will be implemented in the Silver notebook using understandable PySpark expressions and SQL queries.

Examples:

| Dataset | Example check |
|---|---|
| Customers | Required customer identifiers are present |
| Orders | Order ID is unique |
| Orders | Customer reference exists |
| Order items | Price is non-negative |
| Payments | Payment value is non-negative |
| Reviews | Review score is between 1 and 5 |
| Products | Product ID is present |
| Sellers | Seller ID is present |

Invalid records should be retained in a suitable quarantine or rejected-record table for investigation.

Useful execution metrics include:

- Input record count.
- Valid record count.
- Invalid record count.
- Duplicate record count.
- Batch ID.
- Processing timestamp.
- Execution status.

## 8. Orchestration

Databricks Workflows will orchestrate the main processing notebooks:

```text
Python Simulator (separate batch execution)
                  |
                  v
           01_bronze.ipynb
                  |
                  v
           02_silver.ipynb
                  |
                  v
            03_gold.ipynb
                  |
                  v
        Databricks SQL Analytics
```

The simulator can be run before the workflow to generate a new batch. Workflow tasks should have the required dependencies and sensible retry settings.

## 9. Repository Components

The project will keep a small, understandable repository structure:

```text
ecommerce-lakehouse-pipeline/
|
|-- README.md
|-- .gitignore
|
|-- docs/
|   |-- problem_statement.md
|   |-- architecture.md
|
|-- notebooks/
|   |-- 01_bronze.ipynb
|   |-- 02_silver.ipynb
|   |-- 03_gold.ipynb
|
|-- simulator/
|   |-- data_simulator.py
|
|-- sql/
|   |-- analytics.sql
```

Only add another script, notebook, or configuration file when there is a clear implementation need.

## 10. Technology Responsibilities

| Technology | Responsibility |
|---|---|
| AWS S3 | Store historical and synthetic source files, and configured Delta data |
| Azure Databricks | Execute the data engineering pipeline |
| PySpark | Ingest, clean, validate, and transform data |
| Delta Lake | Store reliable Bronze, Silver, and Gold tables |
| Python | Generate synthetic batch data |
| Databricks SQL | Create and query business analytics |
| Databricks Workflows | Orchestrate notebook execution |
| GitHub | Maintain project code and documentation |

## 11. Cost and Security Considerations

- Use small, short-lived Databricks compute for development.
- Stop compute when it is not being used.
- Prefer scheduled batch processing over always-on streaming.
- Avoid repeatedly processing unchanged historical files.
- Give the Databricks identity only the S3 permissions it needs.
- Keep credentials and secrets outside source code and GitHub.
- Verify that storage and checkpoint paths are writable before running the pipeline.

## 12. Implementation Order

1. Inspect the existing local repository, S3 bucket, and Databricks catalog.
2. Finalize the synthetic event schema.
3. Build and run the Python batch simulator.
4. Implement and validate Bronze ingestion.
5. Implement and validate Silver transformations and data quality.
6. Add incremental processing, CDC, and SCD Type 2.
7. Build and validate Gold fact and dimension tables.
8. Create Databricks SQL analytics.
9. Configure and test Databricks Workflows.
10. Complete the README and project documentation.
