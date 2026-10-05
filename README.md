# Enterprise Azure Databricks Data Engineering Project

Enterprise-grade Azure Databricks lakehouse project built to demonstrate end-to-end data engineering using ADLS Gen2, Auto Loader, Delta Lake, Medallion Architecture, CDC, SCD Type 2, Unity Catalog, Lakeflow Jobs, Databricks SQL, data quality, security, monitoring, and performance optimization.

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

