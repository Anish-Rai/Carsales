# Azure Data Engineering Project: ETL Pipeline with SCD Type 1 and incremental load
### Overview
This project demonstrates the implementation of a scalable data engineering solution using Azure Databricks and Delta Lake for SCD (Slowly Changing Dimensions) Type 1 logic. The pipeline follows medallion architecture as it processes raw data from Bronze (raw layer) to Silver (cleansed layer), ensuring data integrity and compliance with historical data requirements.

### Data Pipeline Architecture
![ETL-Architecture](https://github.com/user-attachments/assets/2eb0abec-b134-42bd-ba66-fe0cffbc3392)



### Key Features
* Delta Lake Integration: Ensures ACID compliance and supports efficient data storage and querying.
* Azure Databricks Notebooks: Contains modularized PySpark code for data extraction, transformation, and loading.
* Layered Architecture:
* Bronze: Raw, unprocessed data.
* Silver: Cleansed and enriched data.
* Gold (Future Scope): Aggregated and analytics-ready data.

### Tools and Technologies
* Cloud Platform: Microsoft Azure
* Data Processing: Azure Databricks (PySpark)
* Data Storage: Azure Data Lake Storage (ADLS) with Delta Lake
* ETL Orchestration: Azure Data Factory
* Version Control: GitHub

### Pipeline Details
1. Bronze Layer
Contains raw, unprocessed data directly ingested from various source systems.
Stored in Delta format for scalability.
2. Silver Layer
Cleansed and enriched data processed by the ETL pipeline.
Implements SCD Type 2 logic:
Maintains historical records for tracking changes.
Uses is_current and timestamps for version control.
Converts raw data into CDM-compliant entities.
3. Gold Layer (Future Scope)
Aggregated and analytics-ready data for BI tools.
Optimized for reporting and visualization.

### 🔗 Read the full article on Medium:  
[What I Learned from Building this pipeline]([(https://medium.com/@anish.rai3737/implementing-slowly-changing-dimension-scd-type-1-and-type-2-1097172f54fc)])
