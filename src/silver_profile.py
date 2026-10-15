from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = ROOT / "data" / "bronze" / "power_consumption"

spark = (
    SparkSession.builder
    .appName("SilverProfile")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(BRONZE_PATH))
total = df.count()
print("Total rows:", total)

# Count '?' per column
num_cols = [
    "Global_active_power", "Global_reactive_power",
    "Voltage", "Global_intensity",
    "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
]

for c in num_cols:
    q_count = df.filter(F.col(c) == "?").count()
    print(f"{c}: '?' count = {q_count} ({100*q_count/total:.3f}%)")

# Duplicate Date+Time combos
dups = df.groupBy("Date", "Time").count().filter(F.col("count") > 1).count()
print("Duplicate Date+Time combos:", dups)

# Date range
df.createOrReplaceTempView("bronze")
spark.sql("""
    SELECT MIN(Date) AS min_date, MAX(Date) AS max_date
    FROM bronze
""").show()

spark.stop()