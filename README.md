# Energy Consumption Analysis and Demand Prediction System

Big Data Midterm Project - HAJISH & HARSHVARDHAN

## 1. Problem Statement
Energy consumption varies by hour, day, and season. Utilities need
historical analysis and short-term demand prediction to plan resources.

## 2. Objectives
- Build a Bronze/Silver/Gold data lake pipeline using PySpark.
- Clean and validate 2M+ energy records.
- Analyze hourly, daily, and seasonal consumption patterns.
- Demonstrate PySpark: filter, join, agg, SQL, window functions.
## 2.1 Target Users
- Electricity distribution companies
- Energy planners and grid operators
- Facility managers
- Researchers in energy analytics
- Households tracking consumption patterns
## 2.2 Expected Outcomes
- A reproducible PySpark pipeline with Bronze/Silver/Gold layers.
- Cleaned, validated dataset with 2M+ records.
- Identification of peak-demand hours and seasonal patterns.
- Reports and visualizations for decision-making.
- A demand prediction model with MAE and RMSE metrics.
## 3. Dataset
UCI Individual Household Electric Power Consumption
- Source: https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption
- Records: 2,075,259 minute-level readings
- Period: Dec 2006 - Nov 2010
- Location: one household in Sceaux, France

Hebrail, G., & Berard, A. (2006). Individual Household Electric
Power Consumption. UCI Machine Learning Repository.
https://doi.org/10.24432/C58K54

## 4. Architecture
The project follows the Medallion architecture: Bronze (raw),
Silver (cleaned), Gold (aggregated). Data flows from the raw UCI text file through PySpark processing stages into business-ready Parquet datasets. Details in Section 14.

## 5. Pipeline Layers

| Layer | Rows | Purpose |
|---|---|---|
| Raw | 2,075,259 | source .txt, semicolon-delimited |
| Bronze | 2,075,259 | raw strings + ingestion metadata |
| Silver | 2,049,280 | typed, timestamped, cleaned |
| Quarantine | 25,979 | rows missing active power |
| Gold hourly | 34,168 | hourly aggregates |
| Gold daily | 1,433 | daily aggregates |

## 6. Data Quality Findings
25,979 rows (1.252%) were quarantined due to ? in numeric fields.
- Columns affected: Global_active_power, Global_reactive_power,
  Voltage, Global_intensity, Sub_metering_1, Sub_metering_2.
- Sub_metering_3 has no ? values.
- 0 duplicate timestamps.
- Cleaning rule: keep `?` in Bronze, quarantine in Silver.

## 7. PySpark Operations Demonstrated
| Operation | Location |
|---|---|
| Filter | `process.py` - valid rows |
| Transformation | casts, `date_trunc`, feature columns |
| Aggregation | hourly + daily summaries |
| Spark SQL | 24-hour profile query |
| Window functions | `lag`, rolling 24h avg |
| Join | calendar dimension with season + day type |

## 8. Key Insights
- Peak hour: **20:00** (avg 1.899 kW)
- Lowest hour: **04:00** (avg 0.444 kW)
- Highest season: **Winter Weekend** (avg 1.668 kW)
- Lowest season: **Summer Weekday** (avg 0.705 kW)
- Highest single peak: **2009-02-22** (11.122 kW)

## 9. How to Run
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

python src\ingest.py
python src\silver.py
python src\process.py
python src\verify.py
python src\predict.py
```

## 10. Project Structure
```
energy-consumption-project/
|-- data/
|   |-- raw/
|   |-- bronze/power_consumption/
|   |-- silver/power_consumption/
|   |-- silver/quarantine/
|   \-- gold/{hourly_summary,daily_summary,window_features}/
|-- src/
|   |-- ingest.py
|   |-- silver.py
|   |-- process.py
|   |-- check_bronze.py
|   |-- dashboard.py
|   \-- predict.py
|-- reports/
|-- docs/
|-- requirements.txt
\-- README.md
```

## 11. Tools Used
- Python 3.12, PySpark 3.5.0, Java 17
- Parquet + Medallion architecture
- VS Code, Windows 11

## 12. Limitations
- Single household, not a full grid.
- Minute gaps exist after quarantine.
- Weather data not included.
- Prediction accuracy limited to one household.
- Weather data not included as a predictor..

## 13. Outputs
Reports saved in `reports/`:
- `hourly_profile.csv` - 24-hour consumption curve
- `top_peak_days.csv` - top 10 peak days
- `layer_counts.txt` - row counts per layer
- `prediction_plot.png` - actual vs predicted   chart
- `model_metrics.txt` - MAE, RMSE
- `feature_importance.txt` - top predictors
- `dashboard.png` - 6-panel consumption dashboard
- `prediction_plot.png` - actual vs predicted
- `model_metrics.txt` - MAE, RMSE
- `feature_importance.txt` - top predictors

## 14. Architecture Diagram
```
+-------------+
|  raw .txt   |  UCI dataset (2M rows)
+------+------+
       | read
       v
+---------------------+
|  BRONZE (parquet)   |  raw strings + metadata
+------+--------------+
       | clean, cast, timestamp
       v
+---------------------+
|  SILVER (parquet)   |  typed, feature-engineered
|  + QUARANTINE       |  25,979 bad rows
+------+--------------+
       | filter, agg, SQL, window, join
       v
+---------------------+
|  GOLD (parquet)     |  hourly, daily, window features
+---------------------+
```

