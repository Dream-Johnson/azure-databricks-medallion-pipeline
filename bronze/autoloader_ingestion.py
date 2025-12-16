from pyspark.sql import SparkSession

# Initialize Spark session (handled automatically in Databricks)
spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Configuration
# -----------------------------
RAW_DATA_PATH = "abfss://raw@netflixprojectdlakegl.dfs.core.windows.net"
BRONZE_OUTPUT_PATH = "abfss://bronze@netflixprojectdlakegl.dfs.core.windows.net/netflix_titles"
CHECKPOINT_PATH = "abfss://silver@netflixprojectdlakegl.dfs.core.windows.net/checkpoint"

# -----------------------------
# Incremental ingestion using Auto Loader
# -----------------------------
df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .load(RAW_DATA_PATH)
)

# -----------------------------
# Write to Bronze layer as Delta table
# -----------------------------
(
    df_raw.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start(BRONZE_OUTPUT_PATH)
)
