from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
RAW_FILE = ROOT / "data" / "raw" / "household_power_consumption.txt"
BRONZE_PATH = ROOT / "data" / "bronze" / "power_consumption"

spark = (
    SparkSession.builder
    .appName("EnergyIngest")
    .master("local[2]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Read RAW - keep '?' as literal string. Do NOT convert to null here.
df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", False)
    .csv(str(RAW_FILE))
)

print("Raw row count:", df.count())


# Add ingestion metadata (Bronze = raw + metadata)
bronze = (
    df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingestion_date", F.current_date())
)

# Write Bronze as Parquet
bronze.write.mode("overwrite").parquet(str(BRONZE_PATH))
print("Bronze written to:", BRONZE_PATH)

spark.stop()