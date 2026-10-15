from pathlib import Path
from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Dashboard")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

silver = spark.read.parquet(str(ROOT / "data" / "silver" / "power_consumption"))
hourly = spark.read.parquet(str(ROOT / "data" / "gold" / "hourly_summary"))
daily  = spark.read.parquet(str(ROOT / "data" / "gold" / "daily_summary"))

silver.createOrReplaceTempView("e")

# --- Data pulls ---
hour_profile = spark.sql("""
    SELECT hour, AVG(global_active_power) AS avg_kw
    FROM e GROUP BY hour ORDER BY hour
""").toPandas()

dow_profile = spark.sql("""
    SELECT day_of_week, AVG(global_active_power) AS avg_kw
    FROM e GROUP BY day_of_week ORDER BY day_of_week
""").toPandas()

month_profile = spark.sql("""
    SELECT month, AVG(global_active_power) AS avg_kw
    FROM e GROUP BY month ORDER BY month
""").toPandas()

daily_pd = daily.select("date", "avg_power_kw", "peak_power_kw") \
                .orderBy("date").toPandas()

# --- Dashboard layout: 2 rows x 3 cols ---
fig, axes = plt.subplots(2, 3, figsize=(16, 8))
fig.suptitle("Energy Consumption Dashboard — Household Power Analysis",
             fontsize=14, fontweight="bold")

# 1. Hourly profile
axes[0, 0].plot(hour_profile["hour"], hour_profile["avg_kw"],
                marker="o", color="#1f77b4")
axes[0, 0].set_title("Avg Power by Hour of Day")
axes[0, 0].set_xlabel("Hour")
axes[0, 0].set_ylabel("kW")
axes[0, 0].grid(True, alpha=0.3)

# 2. Day of week
dow_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
axes[0, 1].bar(dow_profile["day_of_week"], dow_profile["avg_kw"],
               color="#2ca02c")
axes[0, 1].set_xticks(range(1, 8))
axes[0, 1].set_xticklabels(dow_labels)
axes[0, 1].set_title("Avg Power by Day of Week")
axes[0, 1].set_ylabel("kW")

# 3. Monthly
axes[0, 2].bar(month_profile["month"], month_profile["avg_kw"],
               color="#ff7f0e")
axes[0, 2].set_title("Avg Power by Month")
axes[0, 2].set_xlabel("Month")
axes[0, 2].set_ylabel("kW")

# 4. Daily trend (avg)
axes[1, 0].plot(pd.to_datetime(daily_pd["date"]), daily_pd["avg_power_kw"],
                color="#d62728", linewidth=0.7)
axes[1, 0].set_title("Daily Avg Power Over Time")
axes[1, 0].set_ylabel("kW")
axes[1, 0].tick_params(axis="x", rotation=30)

# 5. Daily peak
axes[1, 1].plot(pd.to_datetime(daily_pd["date"]), daily_pd["peak_power_kw"],
                color="#9467bd", linewidth=0.7)
axes[1, 1].set_title("Daily Peak Power Over Time")
axes[1, 1].set_ylabel("kW")
axes[1, 1].tick_params(axis="x", rotation=30)

# 6. Prediction plot (if exists)
pred_path = REPORTS / "prediction_plot.png"
if pred_path.exists():
    img = plt.imread(pred_path)
    axes[1, 2].imshow(img)
    axes[1, 2].axis("off")
    axes[1, 2].set_title("Demand Prediction (Test Set)")
else:
    axes[1, 2].text(0.5, 0.5, "No prediction plot yet",
                    ha="center", va="center")
    axes[1, 2].axis("off")

plt.tight_layout()
out = REPORTS / "dashboard.png"
plt.savefig(out, dpi=120)
print("Dashboard saved:", out)

spark.stop()