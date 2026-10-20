# Enterprise Azure Databricks Data Engineering Project

Enterprise Azure Databricks lakehouse project implementing an end-to-end NYC TLC data engineering pipeline using ADLS Gen2, Auto Loader, Delta Lake, Medallion Architecture, Unity Catalog, incremental processing, data quality validation, deduplication, quarantine, and checkpointing.
---

## 1. ADLS Gen2 Data Lake Foundation

### What I built

I created an Azure Data Lake Storage Gen2 account with Hierarchical Namespace enabled and separated the lake into dedicated containers for ingestion, processing, checkpoints, rejected data, and analytics.

### Data Lake Architecture

```text
source-backlog
      ↓
landing
      ↓
Auto Loader
      ↓
bronze
      ↓
silver ──────→ quarantine
      ↓
gold

checkpoints ← Auto Loader / Structured Streaming state
```
### Purpose of each storage zone

| Container | Purpose |
|---|---|
| `source-backlog` | Stores source batches before they are released for ingestion |
| `landing` | Receives newly arriving files for incremental ingestion |
| `checkpoints` | Stores Auto Loader and Structured Streaming processing state |
| `bronze` | Stores raw ingested data with ingestion metadata |
| `silver` | Stores cleaned, validated, deduplicated, and standardized data |
| `quarantine` | Stores rejected or invalid records for investigation |
| `gold` | Stores analytics-ready fact, dimension, and aggregate tables |

### Security and Cost Decisions

- ADLS Gen2 Hierarchical Namespace enabled
- All project containers are private
- Anonymous access disabled
- Microsoft Entra ID used for data-plane authorization
- TLS 1.2 enforced for data in transit
- Storage firewall restricted to selected networks/IPs
- Microsoft-managed keys used for encryption at rest
- LRS selected to minimize development cost while retaining full pipeline functionality

### Implementation Evidence
<img width="2878" height="1376" alt="01_adls_gen2_lake_structure png" src="https://github.com/user-attachments/assets/aeda0e8d-2dd2-4404-8be4-178bcb5d0b88" />

## 2. Secure Databricks-to-ADLS Access

Created an **Azure Databricks Access Connector** with a **system-assigned managed identity** and granted it `Storage Blob Data Contributor` access to the ADLS Gen2 storage account.

```text
Azure Databricks
      ↓
Access Connector
      ↓
Managed Identity
      ↓
Azure RBAC
      ↓
ADLS Gen2
```

### Implementation Evidence
<img width="1704" height="1152" alt="02_access_connector_rbac_assignment png" src="https://github.com/user-attachments/assets/10809abf-570c-4dcd-845e-85b28b3b332b" />

## 3. Source Dataset Scale

The pipeline uses **44.23 GB of compressed NYC Taxi & Limousine Commission (TLC) Parquet data** stored in the ADLS Gen2 source backlog.

The dataset contains:

- **96 Yellow Taxi files**
- **96 FHV files**
- **83 HVFHV files**
- **275 source files total**

The dataset size and file counts are calculated directly from ADLS at runtime rather than hard-coded.

### Implementation Evidence
<img width="2244" height="1276" alt="03_source_dataset_scale png" src="https://github.com/user-attachments/assets/ef6e1ac9-939c-4d1e-ba60-431c01477e3d" />

**Full implementation:** [`00_release_incremental_batch.ipynb`](notebooks/00_release_incremental_batch.ipynb)

### 4. Incremental Bronze Pipeline Orchestration

Created a Databricks **Jobs & Pipelines** workflow to orchestrate the incremental Bronze ingestion process.

The job contains two dependent tasks:

```text
release_incremental_batch
        ↓
bronze_autoloader_ingestion
```

The first task releases the next monthly NYC TLC batch from the source backlog into the landing layer. After it succeeds, the second task uses Databricks Auto Loader to incrementally ingest newly arrived files into Bronze Delta tables.

This run was launched manually to validate the complete task dependency and end-to-end ingestion flow before enabling automated scheduling.

![Successful Bronze Pipeline Run] 
<img width="2784" height="1382" alt="04_incremental_pipeline_success png" src="https://github.com/user-attachments/assets/456b47f0-0e7e-492d-86aa-8d39f043e288" />

**Full implementation:** [`01_bronze_autoloader_ingestion.ipynb`](notebooks/01_bronze_autoloader_ingestion.ipynb)

### 5. Bronze Row Counts Before Incremental Run

Before triggering the next incremental pipeline run, the current Bronze tables contained:

- **Yellow Taxi:** 26,684,795 rows
- **FHV:** 61,144,057 rows
- **HVFHV:** not available yet

HVFHV trip data begins from **February 2019** in the selected source dataset. Since the incremental release process has not yet reached that month, the `fhvhv_trips_raw` Bronze table has not been created yet.

Once the pipeline reaches the first available HVFHV monthly file, the Auto Loader logic will ingest it into Bronze and automatically register:

```text
enterprise_de.bronze.fhvhv_trips_raw
```

![Bronze Row Counts Before Incremental Run]
<img width="2694" height="1488" alt="05_bronze_row_counts_before png" src="https://github.com/user-attachments/assets/4876482e-80e2-4a06-ad85-5684d98102f2" />

#### After Incremental Pipeline Run

After running `nyc_tlc_incremental_pipeline` again, the Bronze row counts increased as the next monthly batch was appended by Auto Loader.

![Bronze Row Counts After Incremental Run]
<img width="2734" height="1518" alt="06_bronze_row_counts_after png" src="https://github.com/user-attachments/assets/b39606cf-2b02-4d82-8adb-fc6141eea9a6" />

## 6. Silver Layer — Taxi Zone Reference Data Ingestion

To support location-based analytics and data quality validation, the official NYC TLC Taxi Zone Lookup dataset was ingested into Azure Data Lake Storage Gen2.

The reference dataset maps numeric taxi location IDs to their corresponding boroughs, zone names, and service zones. Unlike the monthly trip datasets, this lookup is processed as static reference data.

**Implementation workflow:**

`Source CSV → PySpark Ingestion → Data Quality Validation → Delta Lake → Unity Catalog`

### 6.1. Reference Data Quality Validation

Validated all **265 taxi zone records** using PySpark, confirming:

- 0 missing LocationIDs
- 0 duplicate LocationIDs
- 0 missing or empty zone names

**Implementation Evidence 07 — Taxi Zone Data Quality Validation**

<img width="2870" height="1388" alt="07_reference_csv_ingestion png" src="https://github.com/user-attachments/assets/146f803c-c82b-45be-95fc-11d380f8c50b" />


### 6.2. Delta Lake Storage and Unity Catalog Registration

Converted the validated CSV dataset into Delta format and stored it in the ADLS Gen2 Silver container at `silver/taxi_zones/`.

Registered the Delta dataset as an external table in Unity Catalog:

`enterprise_de.silver.taxi_zones`

This provides governed access to the reference data while retaining its physical storage in ADLS Gen2.

**Implementation Evidence 08 — Delta Lake Write and Table Registration**

<img width="2702" height="1306" alt="08_reference_delta_registration png" src="https://github.com/user-attachments/assets/4b108029-62a3-42d6-87f2-8dce876cba39" />


### 6.3. Registered Table Verification

Executed a SQL query against the registered Unity Catalog table to verify successful registration and data accessibility.

The lookup table will support future Silver transformations and Gold analytics by enriching `PULocationID` and `DOLocationID` with descriptive geographic information.

**Implementation Evidence 09 — Unity Catalog SQL Verification**

<img width="2712" height="1320" alt="09_reference_table_verification png" src="https://github.com/user-attachments/assets/3c8cc8fc-2692-4120-b7df-c1c069df9841" />

## 7. Silver Layer — Yellow Taxi Data Transformation

### 7.1. Data Profiling and Duplicate Analysis

Analyzed Bronze Yellow Taxi records using PySpark to identify exact duplicates before Silver transformations. Found **4 duplicate records** using `dropDuplicates()`.

### Implementation Evidence 10 — Yellow Taxi Data Profiling & Duplicate Analysis
<img width="2656" height="714" alt="10_yellow_taxi_data_profiling_duplicate_analysis png" src="https://github.com/user-attachments/assets/a822d600-a8a0-4444-8de2-c5c88858e71a" />

### 7.2. Data Quality and Quarantine

Applied PySpark quality checks for invalid timestamps, negative trip durations, invalid distances, unmapped locations and negative amounts. Separated valid and rejected records using `quarantine_reason`.
Initially loaded 18,931 quarantined records into Delta using overwrite mode. Future incremental batches will use append mode.

### Implementation Evidence 11 — Yellow Taxi Data Quality Logic
<img width="2670" height="1568" alt="11_yellow_taxi_quarantine_logic png" src="https://github.com/user-attachments/assets/7b75e17b-01fe-4a02-b5c1-11c042e13b37" />


### Yellow Taxi Data Quarantine

<img width="2772" height="1220" alt="11a_yellow_taxi_quality_quarantine png" src="https://github.com/user-attachments/assets/92e2010f-8c90-4770-bbff-d6f769785a69" />


### 7.3. Incremental Silver Delta Write

Processed Yellow Taxi micro-batches using `foreachBatch`, removed exact duplicates with `dropDuplicates()`, and wrote the records to Silver Delta with checkpointing.

Verified **35,972,076 records** in the Silver layer.

### Implementation Evidence 12 — Incremental Silver Delta Write

**Silver Streaming Write**

<img width="2650" height="1236" alt="12a_yellow_taxi_silver_streaming_write png" src="https://github.com/user-attachments/assets/ebfad4c3-c9f1-46d5-8f26-8cf86ff7de20" />

**Silver Delta Verification**
<img width="2642" height="1500" alt="12b_yellow_taxi_silver_delta_verification png" src="https://github.com/user-attachments/assets/3d6d1dac-8aad-42f7-a0f2-90f682d8ec4a" />

