# UrbanRide-End-to-End-Lakehouse-Analytics
UrbanRide Lakehouse Data Platform is an end-to-end data engineering project that simulates the data infrastructure of a mid-scale ride-hailing company operating across major cities in India.

> End-to-end Data Engineering & Machine Learning platform built on Databricks for a real ride-hailing company operating across 5 cities in India.

![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-003366?logo=delta)
![MLflow](https://img.shields.io/badge/MLflow-Tracked-0194E2?logo=mlflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0-E25A1C?logo=apachespark)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)

---

## Overview

UrbanRide operates across Mumbai, Delhi, Bengaluru, Hyderabad, and Pune. This project transforms raw operational data — trips, payments, customers, drivers, and clickstream events — into a fully automated ML-powered lakehouse platform.

**What it does:**
- Ingests raw CSV and JSON data daily into Bronze Delta tables
- Cleans, validates, and quality-flags data in Silver
- Builds 25+ ML-ready features per customer in Gold
- Trains 4 ML models — Churn, Fraud, Demand, Segmentation
- Serves insights through 4 SQL dashboards
- Runs everything automatically via 4 Databricks Jobs

---

## Architecture

```
Raw Data (CSV + JSON)
      │
      ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion · Delta format · ACID guarantees
│  Notebooks  │  CSV → overwrite+mergeSchema · JSON → append
│  01, 02     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Cleaned · Validated · Quality-flagged
│  Notebooks  │  Deduplication · MERGE INTO · Never silent drops
│    03–06    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Feature engineered · Partitioned by city
│  Notebooks  │  Z-ordered · OPTIMIZE + VACUUM
│    07–10    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  ML LAYER   │  4 Models · MLflow tracked · Unity Catalog registered
│  Notebooks  │  Churn · Fraud · Demand · Segmentation
│    11–14    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ DASHBOARDS  │  4 SQL Dashboards · Business-ready
│  + JOBS     │  4 Databricks Jobs · Fully automated
└─────────────┘
```

---

## Project Structure

```
urbanride-lakehouse/
│
├── README.md
│
├── notebooks/
│   ├── 01_bronze/
│   │   ├── Urbanride_01_Bronze_CSV_Ingest.ipynb
│   │   └── Urbbanride_02_Bronze_Events_Ingest.ipynb
│   │
│   ├── 02_silver/
│   │   ├── UrbanRide_03_Silver_Customers.ipynb
│   │   ├── UrbanRide_03b_Silver_Drivers.ipynb
│   │   ├── UrbanRide_04_Silver_Trips.ipynb
│   │   ├── UrbanRide_05_Silver_Payments.ipynb
│   │   └── UrbanRide_06_Silver_Clickstream.ipynb
│   │
│   ├── 03_gold/
│   │   ├── UrbanRide_07_Gold_Customer_Features.ipynb
│   │   ├── UrbanRide_08_Gold_Trip_Features.ipynb
│   │   ├── UrbanRide_09_Gold_Payment_Features.ipynb
│   │   └── UrbanRide_10_Gold_Funnel_Metrics.ipynb
│   │
│   └── 04_ml/
│       ├── UrbanRide_11_Churn_Prediction.ipynb
│       ├── UrbanRide_12_Fraud_Detection.ipynb
│       ├── UrbanRide_13_Demand_Forecasting.ipynb
│       └── UrbanRide_14_Customer_Segmentation.ipynb
│
├── dashboards/
│   ├── 01_churn_analysis.sql
│   ├── 02_fraud_monitoring.sql
│   ├── 03_demand_forecasting.sql
│   └── 04_customer_segments.sql
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── ml_model_decisions.md
│
└── assets/
    └── UrbanRide_Lakehouse_Presentation.pptx
```

---

## Data

| Entity | Source | Rows | Format |
|---|---|---|---|
| Customers | CSV | 89,041 | Snapshot daily |
| Drivers | CSV | 500 | Snapshot daily |
| Trips | JSON | ~19.6M | Snapshot daily |
| Payments | CSV | ~19.6M | Snapshot daily |
| Clickstream | JSON | Millions | Append daily |

**Date range:** Sep 1, 2025 → Mar 1, 2026 (182 days)  
**Cities:** Mumbai · Delhi · Bengaluru · Hyderabad · Pune

---

## Unity Catalog Structure

```
urbanride (catalog)
├── bronze (schema)
│   ├── raw_customers
│   ├── raw_drivers
│   ├── raw_trips
│   ├── raw_payments
│   └── raw_events
│
├── silver (schema)
│   ├── customers
│   ├── drivers
│   ├── trips
│   ├── payments
│   ├── clickstream
│   └── Volumes/
│       └── mlflow_artifacts/       ← MLflow staging Volume
│
└── gold (schema)
    ├── customer_features
    ├── trip_features               ← partitioned by city
    ├── payment_features            ← partitioned by city
    ├── funnel_metrics
    ├── churn_predictions
    ├── fraud_predictions
    ├── demand_predictions
    └── customer_segments
```

---

## Gold Layer — Key Features

### customer_features (25 features per customer)

| Feature | Description |
|---|---|
| `days_since_last_trip` | Recency — strongest churn signal |
| `total_trips` | Lifetime trip count |
| `total_spend` | Lifetime spend in INR |
| `cancellation_rate` | Cancelled / total trips |
| `rainy_day_trip_ratio` | Trips on rainy days / total |
| `weekend_trip_ratio` | Weekend trips / total |
| `failed_payment_rate` | Failed payments / total payments |
| `avg_surge_multiplier` | Average surge experienced |
| `favourite_vehicle_type` | Most booked vehicle type |
| `customer_age_days` | Days since signup |

### Optimisation Techniques

| Technique | Applied To | Why |
|---|---|---|
| Partitioning by city | trip_features, payment_features | Every query filters by city — avoids full scans |
| Z-ordering by trip_date | Within city partitions | Co-locates date-range queries |
| OPTIMIZE | After every write | Compacts small Delta files |
| VACUUM | After every write | Removes stale file versions |
| MERGE INTO | Silver tables | Safe upserts — handles reprocessing |

---

## ML Models

### Model 1 — Churn Prediction (Notebook 11 - UrbanRide_11_Churn_Prediction_updated.ipynb)

| | |
|---|---|
| **Type** | Binary Classification |
| **Algorithm** | Random Forest (numTrees=50, maxDepth=10) |
| **Label** | `is_churned` |
| **AUC** | 0.9927 |
| **Recall** | 98% (10,560 / 10,770 churned caught) |
| **Primary metric** | AUC — imbalanced data (12% churn) |
| **Key finding** | `days_since_last_trip` = 45% of feature importance |
| **Output table** | `gold.churn_predictions` |

### Model 2 — Fraud Detection (Notebook 12 - UrbanRide_11_Fraud_Detection.ipynb)

| | |
|---|---|
| **Type** | Binary Classification |
| **Algorithm** | Random Forest (numTrees=50, maxDepth=10) |
| **Label** | `is_suspicious` |
| **AUC** | 0.6989 |
| **Primary metric** | AUC — severe imbalance (0.74% suspicious) |
| **Key lesson** | First run AUC=1.0 — data leakage detected and fixed |
| **Sampling** | 20% stratified sample (19.6M → 3.9M rows) |
| **Class weight** | 68× for suspicious payments |
| **Output table** | `gold.fraud_predictions` |

### Model 3 — Demand Forecasting (Notebook 13 - UrbanRide_11_Demand_Forecasting.ipynb)

| | |
|---|---|
| **Type** | Regression |
| **Algorithm** | Decision Tree Regressor (maxDepth=10) |
| **Label** | `trip_count` (per city per day) |
| **R²** | 0.8519 |
| **RMSE** | 2,837 trips |
| **City error** | <1.5% across all 5 cities |
| **Key finding** | City + day_of_week = 77% of feature importance |
| **Output table** | `gold.demand_predictions` |

### Model 4 — Customer Segmentation (Notebook 14 - UrbanRide_11_Customer_Segmentation.ipynb)

| | |
|---|---|
| **Type** | Clustering (Unsupervised) |
| **Algorithm** | KMeans |
| **Optimal K** | 2 (found via elbow method) |
| **Silhouette** | 0.7263 |
| **Segment 0** | Churned — 6,678 customers · 96 days inactive |
| **Segment 1** | Active — 82,363 customers · 10 days inactive |
| **Key finding** | Unsupervised independently validated supervised churn model |
| **Output table** | `gold.customer_segments` |

---

## Why AUC over Accuracy/F1

On imbalanced datasets:

- A model predicting all customers as active gets **88% accuracy** on churn data — but catches **zero churned customers**
- **F1** is threshold-dependent — evaluates at 0.5 cutoff only
- **AUC** measures ranking ability across all thresholds — cannot be fooled by class imbalance
- In production, the threshold is a business decision (how many at-risk customers can the retention team contact?). AUC evaluates the model independently of that choice.

---

## MLflow & Model Registry

All models follow this registration pattern:

```python
# 1. Infer signature — required by Unity Catalog
signature = infer_signature(sample_input_pandas, sample_output_pandas)

# 2. Log and register
with mlflow.start_run(run_name='register_churn_model') as reg_run:
    mlflow.spark.log_model(
        spark_model   = BEST_MODEL,
        artifact_path = 'churn_model',
        signature     = signature,
        input_example = sample_pandas.head(5),
        dfs_tmpdir    = '/Volumes/urbanride/silver/mlflow_artifacts/mlflow_tmp'
    )
    mv = mlflow.register_model(f'runs:/{reg_run.info.run_id}/churn_model', MODEL_NAME)

# 3. Add description
client.update_model_version(name=MODEL_NAME, version=mv.version, description='...')
```

**Registered models:**
```
urbanride.default.urbanride_churn_model
urbanride.default.urbanride_fraud_model
urbanride.default.urbanride_demand_model
urbanride.default.urbanride_segmentation_model
```

---

## Jobs & Orchestration

| Job | Name | Schedule | Notebooks |
|---|---|---|---|
| 1 | `urbanride_bronze_ingest` | Daily 01:00 AM | 01, 02 |
| 2 | `urbanride_silver_transform` | Daily 02:00 AM | 03, 03b, 04, 05, 06 |
| 3 | `urbanride_gold_build` | Daily 04:00 AM | 07, 08, 09, 10 |
| 4 | `urbanride_ml_training` | Weekly Sunday 06:00 AM | 11, 12, 13, 14 |

**Daily pipeline:**
```
01:00 AM → Bronze ingestion complete
02:00 AM → Silver cleaning complete
04:00 AM → Gold features ready
06:00 AM → ML predictions updated (Sundays only)
Morning  → Dashboards fresh for business teams
```

---

## SQL Dashboards

| Dashboard | Serves | Key Queries |
|---|---|---|
| Churn Analysis | Marketing Team | Risk tier distribution · High risk by city · Retention priority table |
| Fraud Monitoring | Fraud Ops Team | Daily fraud trend · Catch rate · High risk payment queue |
| Demand Forecasting | City Operations | Actual vs predicted · Demand by day of week · City comparison |
| Customer Segments | Marketing & CS | Segment profiles · Churn by segment · Churned high-value list |

---

## Key Technical Decisions

| Decision | Choice | Reason |
|---|---|---|
| File format | Delta Lake | ACID transactions · Schema enforcement · Time travel |
| Bronze write mode (CSV) | Overwrite + mergeSchema | Full daily snapshots · absorbs new columns |
| Bronze write mode (JSON) | Append | Events are additive — history must be preserved |
| Silver write mode | MERGE INTO | Safe upserts — rerunnable without duplicates |
| Gold write mode | Overwrite | Full nightly recompute — simple and reliable |
| ML primary metric | AUC | Imbalanced data — AUC cannot be fooled |
| RF cache strategy | del + gc.collect between fits | Serverless 1GB ML cache hard limit |
| MLflow TMP | UC Volume path | DBFS deprecated — Volumes are the correct artifact store |
| Model path | Full UC path (catalog.schema.model) | Prevents defaulting to workspace.default |

---

## Limitations — Databricks Free Edition

Running on Free Edition means the following are unavailable:

| Feature | Free Edition | Workaround Used |
|---|---|---|
| Serverless ML cache | 1GB hard limit | del model + gc.collect between fits |

---

## Key Insights

1. **Recency beats everything** — `days_since_last_trip` drives 45% of churn prediction power. A customer who stops riding tells you more about their future than all past trips combined.

2. **Perfect scores are dangerous** — AUC=1.0 on fraud detection was data leakage, not performance. `is_fraud_card`, `is_orphan_payment`, `is_fare_mismatch` directly define the label. Always sanity-check perfect results.

3. **Unsupervised validated supervised** — KMeans found Active and Churned segments with no label knowledge. Random Forest found the same groups with full label access. Two independent algorithms, same truth.

4. **City + day = 77% of demand** — The simplest signals are often the strongest. Geography and calendar explain most of ride-hailing demand before any complex feature engineering.

5. **Class imbalance is silent and deadly** — 3 of 4 models had severe imbalance. Without class weights — high accuracy numbers, zero useful predictions. AUC and class weights are non-negotiable on imbalanced data.

---

## Stack

| Component | Technology |
|---|---|
| Platform | Databricks (Free Edition) |
| Storage | Delta Lake |
| Catalog | Unity Catalog |
| ML Tracking | MLflow |
| Processing | Apache Spark 4.0 |
| Language | Python 3.12 |
| Orchestration | Databricks Workflows |
| Dashboards | Databricks SQL |
| ML Algorithms | PySpark MLlib |

---

## Setup

### Prerequisites

1. Databricks workspace (Free Edition or above)
2. Unity Catalog enabled
3. Catalog `urbanride` created
4. Schemas `bronze`, `silver`, `gold` created under `urbanride`
5. Volume `mlflow_artifacts` created under `silver` schema

### Volume Setup

```
Catalog UI → urbanride → silver → Create → Create Volume
Name: mlflow_artifacts
Path: /Volumes/urbanride/silver/mlflow_artifacts/
```

### Running Order

```
Step 1 — Run Urbanride_00 to generate synthetic data
Step 2 — Run Urbanride_01, Urbanride_02 for Bronze ingestion
Step 3 — Run UrbanRide_03 through 06 for Silver
Step 4 — Run UrbanRide_07 through 10 for Gold
Step 5 — Run UrbanRide_11 through 14 for ML models
Step 6 — Set up SQL dashboards using queries in /dashboards
Step 7 — Configure 4 Databricks Jobs for automation
```

### Important Notes

- Always run Cell 0 (session health check) before any ML notebook
- ML notebooks must be run in order — 11 → 12 → 13 → 14
- Delete stale MLflow experiments before rerunning ML notebooks
- `del model + gc.collect()` between LR and RF fits — serverless cache limit

---

## Results Summary

| Model | Metric | Score |
|---|---|---|
| Churn Prediction | AUC | **0.9927** |
| Fraud Detection | AUC | **0.6989** |
| Demand Forecasting | R² | **0.8519** |
| Customer Segmentation | Silhouette | **0.7263** |

```
89,041   customers scored for churn risk
19.6M    payments scored for fraud
910      city-date combinations predicted for demand
2        clean customer segments identified
```

---

## Submitted for

**Databricks Sponsered AI Challenge Oragnised by Codebasics and Indian Data Club**  
Built entirely on Databricks Free Edition · 15 notebooks · End-to-end production platform

---

*Built with Apache Spark · Delta Lake · MLflow · Unity Catalog · Databricks*
