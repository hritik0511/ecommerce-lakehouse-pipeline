# Problem Statement — E-Commerce Lakehouse Data Engineering Platform

## 1. Project Overview

This project builds an end-to-end e-commerce data engineering platform using AWS S3, Azure Databricks, PySpark, Delta Lake, and Databricks SQL.

The platform uses the Olist e-commerce dataset as its historical data source and a Python-based batch simulator to generate additional e-commerce events. It organizes data into Bronze, Silver, and Gold layers using the Medallion Architecture.

The goal is to demonstrate a practical data engineering workflow: ingesting raw data, improving data quality, processing incremental changes, maintaining historical records, and preparing data for business analytics.

## 2. Business Problem

An e-commerce marketplace generates data across several business areas, including customers, orders, products, sellers, payments, and customer reviews.

When these datasets are maintained separately, it becomes difficult to:

- Combine information from different business areas.
- Identify duplicate, incomplete, or invalid records.
- Process newly arriving data without repeatedly processing all historical data.
- Track changes to customer and seller information over time.
- Produce consistent metrics for sales, payments, delivery performance, and customer satisfaction.

The business needs a centralized data platform that turns raw operational data into reliable, analytics-ready datasets.

## 3. Proposed Solution

Build a Lakehouse Data Engineering Platform with the following capabilities:

1. **Data ingestion:** Read the historical Olist CSV datasets from AWS S3 into Databricks.
2. **Raw data storage:** Store ingested data as Bronze Delta tables while retaining useful source and ingestion metadata.
3. **Data transformation:** Clean and standardize data in the Silver layer using PySpark.
4. **Data quality:** Check required fields, data types, duplicate records, valid values, and important business relationships. Keep invalid records available for investigation rather than silently discarding them.
5. **Incremental processing:** Ingest newly generated batch files without reprocessing all previously ingested files.
6. **Change processing:** Use simulator-generated insert, update, and delete events to demonstrate Change Data Capture (CDC).
7. **Historical tracking:** Use Slowly Changing Dimension Type 2 (SCD Type 2) to preserve changes to selected customer and seller attributes.
8. **Analytics modelling:** Build fact and dimension tables in the Gold layer.
9. **Business analytics:** Use Databricks SQL to calculate business metrics.
10. **Orchestration:** Use Databricks Workflows to run the notebooks in the required order.

## 4. Data Sources

### Historical data

The project uses the public Olist e-commerce dataset, which contains related datasets such as:

- Customers
- Orders
- Order items
- Payments
- Reviews
- Products
- Sellers
- Geolocation
- Product category translations

The actual files, schemas, and record counts will be inspected and validated during implementation.

### Synthetic data

A small Python simulator will generate additional batch files representing new business activity and changes. Examples include:

- New orders and payments.
- Customer or seller attribute updates.
- Order status changes.
- Duplicate events.
- Invalid records for data-quality checks.
- Delete events represented as explicit event records.

Synthetic events are used to demonstrate incremental processing and CDC. They are not presented as original Olist source data or as actual CDC logs from Olist.

## 5. Expected Business Outcomes

The Gold layer should support analysis such as:

- Revenue and order trends over time.
- Average order value.
- Top-selling product categories.
- Seller performance.
- Payment method distribution.
- Delivery duration and delayed deliveries.
- Repeat customer activity.
- Customer review scores.

## 6. Project Scope

### In scope

- AWS S3 as the source and storage location.
- Azure Databricks for data processing.
- PySpark transformations.
- Delta Lake tables.
- Bronze, Silver, and Gold layers.
- Python batch data simulator.
- Data-quality checks and duplicate handling.
- Incremental ingestion.
- CDC using simulated change events.
- SCD Type 2 for selected entities.
- Fact and dimension modelling.
- Databricks SQL analytics.
- Databricks Workflows.
- Basic execution and project documentation.


## 7. Technology Stack

| Technology | Purpose |
|---|---|
| AWS S3 | Store historical and generated source files |
| Azure Databricks | Data engineering workspace and execution |
| PySpark | Data ingestion and transformation |
| Delta Lake | Reliable storage for Bronze, Silver, and Gold tables |
| Python | Generate synthetic batch data |
| Databricks SQL | Query Gold tables and calculate business metrics |
| Databricks Workflows | Schedule and orchestrate pipeline execution |
| GitHub | Version control and project documentation |

## 8. Success Criteria

The project will be considered complete when:

- Historical source datasets are ingested successfully.
- Bronze, Silver, and Gold Delta tables are created and validated.
- New simulator batches can be ingested incrementally.
- Reprocessing a batch does not create duplicate business records.
- Invalid records can be identified and inspected.
- CDC operations are demonstrated using simulator-generated events.
- SCD Type 2 preserves previous and current versions of selected entities.
- Gold fact and dimension tables produce consistent analytical results.
- Databricks Workflows can execute the pipeline in the correct order.
- The repository explains the architecture, execution steps, and key design decisions.
