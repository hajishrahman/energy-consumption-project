from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = ROOT / "data" / "silver" / "power_consumption"
GOLD_HOURLY = ROOT / "data" / "gold" / "hourly_summary"
GOLD_DAILY = ROOT / "data" / "gold" / "daily_summary"
GOLD_WINDOW = ROOT / "data" / "gold" / "window_features"

spark = (
    SparkSession.builder
    .appName("EnergyProcess")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(SILVER_PATH))
print("Silver rows:", df.count())

# ========================================================
# A. FILTERING + TRANSFORMATIONS
# ========================================================
valid_df = df.filter(
    (F.col("global_active_power").isNotNull())
    & (F.col("global_active_power") >= 0)
    & (F.col("voltage") > 0)
)
print("After filter:", valid_df.count())

# Truncate timestamp to hour — creates a proper hourly grid key
valid_df = valid_df.withColumn(
    "hour_bucket", F.date_trunc("hour", F.col("timestamp"))
)

# ========================================================
# B. AGGREGATION → hourly summary (Gold)
# ========================================================
hourly = (
    valid_df
    .groupBy("hour_bucket")
    .agg(
        F.avg("global_active_power").alias("avg_power_kw"),
        F.max("global_active_power").alias("peak_power_kw"),
        F.min("global_active_power").alias("min_power_kw"),
        F.count("*").alias("measurement_count"),
        # Energy in kWh — sum(power_kW) * (1/60 h) per minute reading
        (F.sum("global_active_power") / 60.0).alias("energy_kwh")
    )
)
print("Hourly buckets:", hourly.count())
hourly.orderBy("hour_bucket").show(5, truncate=False)
hourly.write.mode("overwrite").parquet(str(GOLD_HOURLY))

# ========================================================
# C. AGGREGATION → daily summary (Gold)
# ========================================================
daily = (
    valid_df
    .withColumn("date", F.to_date("timestamp"))
    .groupBy("date")
    .agg(
        F.avg("global_active_power").alias("avg_power_kw"),
        F.max("global_active_power").alias("peak_power_kw"),
        F.sum("global_active_power").alias("total_power_kw"),
        F.count("*").alias("measurement_count")
    )
)
daily.orderBy(F.desc("peak_power_kw")).show(10)
daily.write.mode("overwrite").parquet(str(GOLD_DAILY))

# ========================================================
# D. SPARK SQL — peak hour profile (24-hour curve)
# ========================================================
valid_df.createOrReplaceTempView("energy")
hour_profile = spark.sql("""
    SELECT
        hour,
        ROUND(AVG(global_active_power), 3) AS avg_power_kw,
        ROUND(MAX(global_active_power), 3) AS peak_power_kw,
        COUNT(*) AS records
    FROM energy
    GROUP BY hour
    ORDER BY hour
""")
hour_profile.show(24)

# ========================================================
# E. WINDOW FUNCTIONS — LAG on hourly data (safe: regular grid)
# ========================================================
w = Window.orderBy("hour_bucket")

windowed = (
    hourly
    .withColumn("prev_hour_power", F.lag("avg_power_kw", 1).over(w))
    .withColumn("power_change",
                F.col("avg_power_kw") - F.col("prev_hour_power"))
    .withColumn("rolling_avg_24h",
                F.avg("avg_power_kw").over(w.rowsBetween(-23, 0)))
)
windowed.orderBy("hour_bucket").show(10, truncate=False)
windowed.write.mode("overwrite").parquet(str(GOLD_WINDOW))

# ========================================================
# F. JOIN — real enrichment: calendar dimension built separately
# ========================================================
# Build a calendar dim with weekday/weekend + season (real lookup, not self-join)
calendar = (
    daily.select("date")
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("dow", F.dayofweek("date"))
    .withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "Winter")
         .when(F.col("month").isin(3, 4, 5), "Spring")
         .when(F.col("month").isin(6, 7, 8), "Summer")
         .otherwise("Autumn")
    )
    .withColumn(
        "day_type",
        F.when(F.col("dow").isin(1, 7), "Weekend").otherwise("Weekday")
    )
)

daily_enriched = daily.join(calendar, on="date", how="left")
print("Joined rows:", daily_enriched.count())
daily_enriched.show(10, truncate=False)

# Season aggregate (proves the join added value)
daily_enriched.groupBy("season", "day_type").agg(
    F.round(F.avg("avg_power_kw"), 3).alias("avg_kw"),
    F.round(F.avg("peak_power_kw"), 3).alias("peak_kw")
).orderBy("season", "day_type").show()

print("Done. Gold outputs written.")
spark.stop()