# 🏥 Databricks Medallion Architecture: Biostatistics Discharges (DLT Edition)

This repository contains a robust data engineering pipeline for processing hospital discharge records (Biostatistics) using **Delta Live Tables (DLT)**. The project implements a native **Medallion Architecture** (Bronze/Raw, Silver, Gold) orchestrated on Databricks via Databricks Asset Bundles (DABs).

💡 **Note on origin:** This project is another implementation and evolution of the data quality architecture model originally presented in [this LinkedIn post](https://www.linkedin.com/feed/update/urn:li:activity:7516470343470026752/).

## 🏗️ Pipeline Architecture

The data flow is designed as a declarative, continuous pipeline graph (*DAG*) managed natively by Delta Live Tables, ensuring automated dependency resolution and incremental processing.

1. **Bronze (Raw Ingestion - `load_discharges_raw.py`):** Extracts raw multi-year CSV discharge datasets from Azure Blob Storage (`abfss://`) using explicit schemas. It standardizes divergent historical formats from 7 distinct groups into a single unified Delta table (`discharges_raw_dlt`).

2. **Silver (Transformation & Cleansing - `load_discharges_silver.py`):** Cleans dates, fills nulls, imputes missing values, and applies native DLT data quality expectations (`@dlt.expect_all_or_drop`) to guarantee data integrity before materializing `discharges_silver_dlt`.

3. **Gold (Star Schema Modeling - `load_discharges_gold.py`):** Joins the Silver table with dimension tables (Gender, Age, Year, Health Insurance, Healthcare Service, City, and Cause of Death lookups) stored in Azure Data Lake, recodes causes of death, validates geographic codes (CUT), and produces the final fact table (`discharges_gold_dlt`).

## 🛠️ Tech Stack

* **Processing Engine:** Databricks Delta Live Tables (DLT) on Serverless Compute (`PERFORMANCE_OPTIMIZED`).
* **Orchestration & Deployment:** Databricks Asset Bundles (DABs).
* **Data Quality:** Native DLT Expectations (`@dlt.expect`, `@dlt.expect_all_or_drop`).
* **Storage & Governance:** Unity Catalog, Azure Data Lake Storage Gen2 (ABFSS).
* **Language & Core:** Python 3, PySpark.

## 🚀 Key Technical Decisions

### 1. Declarative DLT Architecture & Automated DAG
Moving away from sequential workflow tasks, the project leverages DLT decorators (`@dlt.table`) to define tables declaratively. The DLT engine automatically analyzes dependencies and builds the execution graph, handling data streaming and updates seamlessly.

### 2. Environment & Configuration Injection (`.env` → DABs → Spark)
Hardcoded configurations and credentials are entirely eliminated. Variables such as storage accounts and containers are managed locally in an `.env` file, loaded via DABs (`databricks.yml`), passed securely to the pipeline via the `configuration` block in `discharges_pipeline.yml`, and accessed dynamically at runtime using `spark.conf.get()`.

### 3. Native Data Quality Expectations
Data validation is embedded directly into the transformation definitions using DLT expectations (e.g., `@dlt.expect_all_or_drop`), automatically quarantining or dropping invalid records based on strict business and range rules.

### 4. Modular Shared Libraries
Core extraction and connector logic are decoupled into reusable utility modules (`src/lib/storage_utils.py`), leveraging `SparkSession.getActiveSession()` to fetch active environment configurations dynamically.

## ☁️ Databricks Deployment

To deploy and run the DLT pipeline using the Databricks CLI:

```bash
# Deploy the bundle configuration and pipeline definition to development
databricks bundle deploy -t dev

# Trigger an update of the DLT pipeline
databricks bundle run discharges_dlt_pipeline -t dev
```