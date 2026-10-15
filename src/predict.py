from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
GOLD_HOURLY = ROOT / "data" / "gold" / "hourly_summary"
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

spark = (
    SparkSession.builder
    .appName("EnergyPredict")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# =========================================================
# 1. Load Gold hourly summary
# =========================================================
hourly = spark.read.parquet(str(GOLD_HOURLY))
print("Hourly rows:", hourly.count())

# =========================================================
# 2. Build lag + rolling features
# =========================================================
w = Window.orderBy("hour_bucket")

features_df = (
    hourly
    .withColumn("lag_1h", F.lag("avg_power_kw", 1).over(w))
    .withColumn("lag_24h", F.lag("avg_power_kw", 24).over(w))
    .withColumn("rolling_avg_24h",
                F.avg("avg_power_kw").over(w.rowsBetween(-23, 0)))
    .withColumn("hour", F.hour("hour_bucket"))
    .withColumn("day_of_week", F.dayofweek("hour_bucket"))
    .withColumn("month", F.month("hour_bucket"))
    .withColumn("target", F.col("avg_power_kw"))
    .dropna(subset=["lag_1h", "lag_24h", "rolling_avg_24h"])
)

print("After lag features:", features_df.count())

# =========================================================
# 3. Convert to Pandas for sklearn
# =========================================================
pdf = features_df.select(
    "hour_bucket", "hour", "day_of_week", "month",
    "lag_1h", "lag_24h", "rolling_avg_24h", "target"
).toPandas()

pdf = pdf.sort_values("hour_bucket").reset_index(drop=True)
print("Pandas rows:", len(pdf))

# =========================================================
# 4. Time-based train/test split (80/20)
# =========================================================
split_idx = int(len(pdf) * 0.8)
train = pdf.iloc[:split_idx]
test = pdf.iloc[split_idx:]

print(f"Train rows: {len(train)} | Test rows: {len(test)}")

feature_cols = ["hour", "day_of_week", "month",
                "lag_1h", "lag_24h", "rolling_avg_24h"]

X_train = train[feature_cols]
y_train = train["target"]
X_test = test[feature_cols]
y_test = test["target"]

# =========================================================
# 5. Train Random Forest
# =========================================================
model = RandomForestRegressor(
    n_estimators=100,
    max_depth=12,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)
print("Model trained.")

# =========================================================
# 6. Evaluate
# =========================================================
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
rmse = np.sqrt(mean_squared_error(y_test, preds))

print(f"MAE:  {mae:.4f} kW")
print(f"RMSE: {rmse:.4f} kW")

with open(REPORTS / "model_metrics.txt", "w") as f:
    f.write("Demand Prediction Model — Random Forest\n")
    f.write("=" * 45 + "\n")
    f.write(f"Features: {feature_cols}\n")
    f.write(f"Train rows: {len(train)}\n")
    f.write(f"Test rows:  {len(test)}\n")
    f.write(f"MAE:  {mae:.4f} kW\n")
    f.write(f"RMSE: {rmse:.4f} kW\n")

# =========================================================
# 7. Actual vs Predicted plot
# =========================================================
plt.figure(figsize=(12, 5))
plt.plot(test["hour_bucket"].values, y_test.values,
         label="Actual", linewidth=1.2)
plt.plot(test["hour_bucket"].values, preds,
         label="Predicted", linewidth=1.2, alpha=0.85)
plt.xlabel("Time")
plt.ylabel("Average Power (kW)")
plt.title("Demand Prediction — Actual vs Predicted (Test Set)")
plt.legend()
plt.tight_layout()
plt.savefig(REPORTS / "prediction_plot.png", dpi=120)
print("Plot saved:", REPORTS / "prediction_plot.png")

# =========================================================
# 8. Feature importance
# =========================================================
importances = pd.Series(model.feature_importances_, index=feature_cols)
importances = importances.sort_values(ascending=False)
print("\nFeature importance:")
print(importances)

with open(REPORTS / "feature_importance.txt", "w") as f:
    f.write("Feature Importance\n")
    f.write("=" * 30 + "\n")
    for name, score in importances.items():
        f.write(f"{name}: {score:.4f}\n")

spark.stop()