from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = ROOT / "data" / "bronze" / "power_consumption"

spark = (
    SparkSession.builder
    .appName("CheckBronze")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(BRONZE_PATH))
print("Bronze row count:", df.count())
print("Columns:", df.columns)
df.show(5, truncate=False)

spark.stop()