# 🏥 Databricks Medallion Architecture: Biostatistics Discharges

This repository contains a robust data engineering pipeline for processing hospital discharge records (Biostatistics). The project implements a **Medallion Architecture** (Bronze/Raw, Silver, Gold) orchestrated on Databricks.

💡 **Note on origin:** This project is another implementation and evolution of the data quality architecture model originally presented in [this LinkedIn post](https://www.linkedin.com/feed/update/urn:li:activity:7516470343470026752/).

## 🏗️ Pipeline Architecture

The data flow is designed as a sequence of dependent tasks (*Databricks Workflows*) using pure PySpark, which provides precise control over transformations and validations.

1. **Bronze (Raw Ingestion):** Extracts raw data from Azure Blob Storage (`abfss://`). It standardizes divergent schemas from 7 distinct historical discharge groups and unifies them into a single Delta table.

2. **Raw Data Quality:** Baseline expectation tests (using Great Expectations) on the unified raw table.

3. **Silver Transformation:** Data cleansing, data type casting, and null handling.

4. **Silver Data Quality:** Strict validation using regular expressions (for ICD-10 codes) and logical ranges for age and other identifiers.

5. **Gold Transformation:** Star Schema modeling. Joins the Silver table with dimensions stored in Azure Data Lake to obtain final IDs (referential integrity), recodes causes of death, and validates city codes (CUT).

6. **Gold Data Quality:** Referential integrity verification. Ensures all foreign keys in the fact table exist in their respective dimensions.

## 🛠️ Tech Stack

* **Processing:** Databricks Serverless Compute (`PERFORMANCE_OPTIMIZED`), PySpark.
* **Orchestration:** Databricks Asset Bundles (DABs), Databricks Workflows / Jobs.
* **Data Quality (DQ):** Great Expectations (GX).
* **Storage:** Unity Catalog, Azure Blob Storage (Data Lake).
* **Language:** Python 3, CLI scripting (`argparse`).

## 🚀 Key Technical Decisions

### 1. Great Expectations in Serverless Environments

Databricks Serverless utilizes Spark Connect under the hood, which presents restrictions with native Spark caching commands (`persist()`, `cache()`). To integrate GX successfully without raising *JVM/AnalysisException* errors, GX contexts are instantiated in-memory and the Datasource persistence was explicitly disabled:

```python
datasource = context.data_sources.add_or_update_spark(
    name="spark_datasource",
    persist=False # Fixes Serverless compatibility issues
)
```

### 2. Databricks Asset Bundles (DABs) & Variable Injection

Hardcoded credentials or paths in the Python code were avoided. Variables such as the Unity Catalog, schemas, and Azure Storage containers are dynamically injected into the Job via CLI parameters configured in the `databricks.yml` file.

### 3. Graceful Task Bypassing

A `--skip-validation` flag was implemented to facilitate local and iterative development. Instead of using `sys.exit(0)` (which Databricks interprets as an error, triggering automatic retries), the bypass executes an early `return None`. This allows the task to finish gracefully and the Workflow to proceed to the next step:

```python
if skip_validation.lower() == "true":
    print("⏭️ Skipping step as requested.")
    return None
```

### 4. Modular Project Structure

The code is organized to facilitate maintainability and dynamic Python path resolution:

* `src/`: Contains the processing scripts (PySpark ETL).
* `tests/`: Contains the Great Expectations validation suites.
* `src/lib/`: Shared utility modules, such as unified connectors for Azure Blob Storage.

## 💻 Local Execution (Development)

Thanks to the `argparse` design and passing paths via the console, you can test the transformation or validation logic in your local environment by installing PySpark and GX, and injecting a sample Parquet file:

```bash
python tests/test_discharges_silver.py \
  --input-source ./sample_data/discharges.parquet \
  --skip-validation false
```

## ☁️ Databricks Deployment

To deploy the pipeline to the development environment using the Databricks CLI:

```bash
databricks bundle deploy -t dev
databricks bundle run discharges_job -t dev
```

**[Delta Live Table (DLT) version can be found here](https://github.com/jonmunm/biostatistics-3tier-olap-databricks/tree/origin/dlt)**