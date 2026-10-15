from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = ROOT / "data" / "bronze" / "power_consumption"
SILVER_PATH = ROOT / "data" / "silver" / "power_consumption"
QUARANTINE_PATH = ROOT / "data" / "silver" / "quarantine"

spark = (
    SparkSession.builder
    .appName("EnergySilver")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.parquet(str(BRONZE_PATH))
bronze_count = bronze.count()
print(f"Bronze rows: {bronze_count}")

# 1. Build timestamp
df = bronze.withColumn(
    "timestamp",
    F.to_timestamp(
        F.concat_ws(" ", F.col("Date"), F.col("Time")),
        "d/M/yyyy H:mm:ss"
    )
)

# 2. Cast numeric columns
num_map = {
    "Global_active_power": "global_active_power",
    "Global_reactive_power": "global_reactive_power",
    "Voltage": "voltage",
    "Global_intensity": "global_intensity",
    "Sub_metering_1": "sub_metering_1",
    "Sub_metering_2": "sub_metering_2",
    "Sub_metering_3": "sub_metering_3",
}
for src, dst in num_map.items():
    df = df.withColumn(dst, F.col(src).cast("double"))

# 3. Split valid vs quarantine (missing active power OR bad timestamp)
bad_condition = (
    F.col("timestamp").isNull()
    | F.col("global_active_power").isNull()
    | (F.col("global_active_power") < 0)
    | (F.col("voltage") < 100) | (F.col("voltage") > 300)
    | (F.col("global_intensity") < 0)
    | (F.col("sub_metering_1") < 0)
    | (F.col("sub_metering_2") < 0)
    | (F.col("sub_metering_3") < 0)
)

quarantine = df.filter(bad_condition)
silver = df.filter(~bad_condition)

# 4. Drop duplicates on timestamp
silver = silver.dropDuplicates(["timestamp"])

# 5. Time features
silver = (
    silver
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("minute", F.minute("timestamp"))
    .withColumn("day_of_week", F.dayofweek("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("year", F.year("timestamp"))
)

# 6. Select only needed columns
silver_final = silver.select(
    "timestamp",
    "global_active_power",
    "global_reactive_power",
    "voltage",
    "global_intensity",
    "sub_metering_1",
    "sub_metering_2",
    "sub_metering_3",
    "hour", "minute", "day_of_week", "month", "year"
)

silver_final.agg(
    F.min("timestamp").alias("min_ts"),
    F.max("timestamp").alias("max_ts")
).show()

silver_count = silver_final.count()
quarantine_count = quarantine.count()

print(f"Silver rows:      {silver_count}")
print(f"Quarantine rows:  {quarantine_count}")
print(f"Total accounted:  {silver_count + quarantine_count}")
print(f"Bronze rows:      {bronze_count}")

silver_final.write.mode("overwrite").parquet(str(SILVER_PATH))
quarantine.write.mode("overwrite").parquet(str(QUARANTINE_PATH))

print("Silver written to:", SILVER_PATH)
print("Quarantine written to:", QUARANTINE_PATH)

silver_final.show(5, truncate=False)

spark.stop()