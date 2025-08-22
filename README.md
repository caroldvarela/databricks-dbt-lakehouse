# 🏗️ Data Lakehouse Platform with CDC/SCD using Databricks + DBT

This project implements a unified data platform under the **Data Lakehouse** paradigm, using **Databricks** as the execution engine and **DBT Cloud** for analytical modeling.  
The architecture is based on the **Delta Lake** format and the **Medallion pattern (Bronze → Silver → Gold)** to ensure data quality, traceability, and consistency.

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-%23009988.svg?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![LakeFlow Declarative Pipelines](https://img.shields.io/badge/LakeFlow%20Declarative%20Pipelines-0073E6?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/product/lakeflow)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://www.python.org/)

![image](https://github.com/caroldvarela/images/blob/main/databricks-dbt.png)

---

## 📋 Table of Contents

1. Objective  
2. Scope
3. Architecture
4. Data Sources
5. ETL Processes
6. Business Models (DBT)
7. Technologies Used
8. Project Structure
9. Execution
10. Acknowledgements
    
---

## 🎯 Objective

Build a Data Lakehouse Platform in Databricks that:

- Ensures **quality, traceability, and consistency** of the data.  
- Supports **business analytics and advanced reporting** through DBT.  
- Allows **incremental loads** and handles **Slowly Changing Dimensions (SCD)** and **Change Data Capture (CDC)**.  

---

## 📖 Scope

Covers the full processing lifecycle:

- Ingestion of CSV files into Delta Lake.  
- Cleaning, standardization, and application of quality rules.  
- Loading of dimensions and facts with **SCD Type 1** strategies.  
- Incremental and CDC-based loading of the fact table.
- Development of analytical models in DBT.

---

## 🏛️ Architecture

### Medallion Layers

| Layer | Purpose | Technology |
|---|---|---|
| **Raw** | Storage of raw source files | Databricks Volumes |
| **Bronze** | Automated ingestion with Autoloader, Delta tables with flexible schema | Databricks Autoloader + Delta Lake |
| **Silver** | Cleaning, typing, validations, and CDC | Delta Lake + LakeFlow Declarative Pipelines (CDC, quality rules, incremental loads) |
| **Gold** | Optimized dimensional model (facts and dimensions) | Delta Lake (MERGE for CDC, SCD Type 1) |
---

## 📂 Data Sources

Source CSV files:

- **Dimensions**: Customers, Products, Stores, Salespersons, Campaigns, Dates, Times.  
- **Facts**: Sales.  

CSV files feeding the transformation pipeline, organized in three types:

- **Initial data** (`dim_campaigns.csv`, `dim_products.csv`, etc.): Base and static datasets used to initialize dimensions and facts in the Raw/Bronze layers.
- **Incremental data** (`*_increment.csv`): New records appended over time, without modifying existing ones (supports incremental loading).
- **SCD data** (`*_scd.csv`): Updates in dimension attributes (e.g., customer, product, campaign changes), processed with SCD Type 1 to always keep the most up-to-date version.  

These datasets come from Kaggle ([Link](https://www.kaggle.com/datasets/shrinivasv/retail-store-star-schema-dataset/data)) adapted to our use case.  

Each file is stored in the `Raw` layer and transformed into `Silver` and `Gold` tables.  

---

## ⚙️ ETL Processes

### Main Notebooks:

| Notebook | Purpose |
|---|---|
| `Setup.ipynb` | Schema and volume creation |
| `Bronze.ipynb` | Raw → Bronze ingestion with Autoloader |
| `DLT_pipeline.py` | Bronze → Silver transformation with DLT and CDC |
| `LoadStaticDimensions.ipynb` | Load of static dimensions (dates and times) |
| `GoldLayer-Dimensions.ipynb` | Load of incremental dimensions with SCD Type 1 + CDC via MERGE |
| `GoldLayer-Fact.ipynb` | Incremental load of the FactSales table with CDC |

---

## 📊 Business Models (DBT)

### Implemented models:

1. `campaignsalessummary` → Campaign sales summary  
2. `customersegmentsalessummary` → Sales by customer segment  
3. `dailycampaignsales` → Daily sales by campaign  
4. `dailymonthlysalessummary` → Daily and monthly sales summary  
5. `dailystoresales` → Daily sales by store  
6. `monthlysalesbycategory` → Monthly sales by category  
7. `monthlysalesbysalesperson` → Monthly sales by salesperson  
8. `salespersonsalessummary` → Salesperson performance  
9. `salessummarybycategorybrand` → Sales by category and brand  
10. `storesalessummary` → Store sales summary  

### Business questions answered:

- Impact of campaigns on sales  
- Customer segmentation by income  
- Evolution of daily/monthly sales  
- Performance by store, salesperson, and product  

---

## 🛠️ Technologies Used

- **Databricks**
- **Python** / **PySpark**
- **Delta Lake**  
- **Autoloader** / **cloudFiles** 
- **Lakeflow Declarative Pipelines (previously known as Delta Live Tables (DLT))**
- **Unity Catalog**
- **DBT Cloud**  
 

---

## 📁 Project Structure


A good way to format a file tree in a README is to use a code block with proper indentation. This makes the structure clear and prevents the formatting from breaking on different platforms, such as GitHub.

Here is a corrected and well-formatted version of the file tree you provided:

```
.
├── notebooks/
│   ├── 01_Setup.ipynb            # Create schemas and volumes
│   ├── 02_SrcParameters.ipynb    # Source configuration
│   ├── 03_Bronze.ipynb           # Raw → Bronze ingestion
│   ├── 04_DLT_pipeline.py        # Bronze → Silver transformation
│   ├── 05_LoadStaticDimensions.ipynb  # Static dimensions load
│   ├── 06_DimParameters.ipynb    # Dimensions configuration
│   ├── 07_GoldLayer-Dimensions.ipynb  # SCD dimensions processing
│   └── 08_GoldLayer-Fact.ipynb   # Fact table CDC load
│
├── dbt/
│   ├── models/
│   │   └── business/
│   │       ├── campaignsalessummary.sql
│   │       ├── dailycampaignsales.sql
│   │       ├── dailystoresales.sql
│   │       ├── monthlysalesbycategory.sql
│   │       ├── schema.yml          # DBT schema configuration
│   │       └── ...
│   └── dbt_project.yml           # DBT project configuration
│
├── data/
│   ├── dim_campaigns.csv
│   ├── dim_campaigns_increment.csv
│   ├── dim_campaigns_scd.csv
│   ├── dim_customers.csv
│   ├── dim_customers_increment.csv
│   ├── dim_customers_scd.csv
│   ├── dim_dates.csv
│   ├── fact_sales.csv
│   ├── fact_sales_increment.csv
│   └── ...
│
└── docs/
    └── Documentation.pdf         # Full project documentation
```
---

## ▶️ Execution

1. Run `Setup.ipynb` to initialize schemas and volumes.  
2. Upload CSV files to the Raw layer.  
3. Run Bronze ingestion job (`Bronze.ipynb`).  
4. Run DLT pipeline for Silver (`DLT_pipeline.py`).  
5. Load static dimensions (`LoadStaticDimensions.ipynb`).  
6. Load incremental dimensions (`GoldLayer-Dimensions.ipynb`).  
7. Load fact table (`GoldLayer-Fact.ipynb`).  
8. Run DBT models on the Gold tables.  


---

### Acknowledgements
I would like to express my deep gratitude to [Ansh Lamba](https://www.youtube.com/@AnshLamba) for his tutorial ["DATABRICKS x DBT End-To-End Data Engineering Project"](https://www.youtube.com/watch?v=vT7Oeu7WqHg), which served as the main inspiration for this work.

While the tutorial served as a foundation, this implementation diverges in several ways due to the use of a different dataset.

This work would not have been possible without his clear explanations, shared knowledge, and generosity with the data community.
