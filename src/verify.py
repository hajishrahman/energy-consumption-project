from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]

spark = (
    SparkSession.builder
    .appName("Verify")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

checks = {
    "Bronze":   ROOT / "data" / "bronze" / "power_consumption",
    "Silver":   ROOT / "data" / "silver" / "power_consumption",
    "Quarantine": ROOT / "data" / "silver" / "quarantine",
    "Gold hourly": ROOT / "data" / "gold" / "hourly_summary",
    "Gold daily":  ROOT / "data" / "gold" / "daily_summary",
    "Gold window": ROOT / "data" / "gold" / "window_features",
}

for name, path in checks.items():
    if path.exists():
        count = spark.read.parquet(str(path)).count()
        print(f"{name:15s} OK   rows = {count}")
    else:
        print(f"{name:15s} MISSING")

spark.stop()