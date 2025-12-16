from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Configuration
# -----------------------------
SOURCE_CONTAINER = "abfss://bronze@netflixprojectdlakegl.dfs.core.windows.net"
TARGET_CONTAINER = "abfss://silver@netflixprojectdlakegl.dfs.core.windows.net"

SOURCE_FOLDER = "netflix_directors"
TARGET_FOLDER = "netflix_directors"

# -----------------------------
# Read data from Bronze layer
# -----------------------------
df_lookup = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{SOURCE_CONTAINER}/{SOURCE_FOLDER}")
)

# -----------------------------
# Write data to Silver layer as Delta
# -----------------------------
(
    df_lookup.write
    .format("delta")
    .mode("append")
    .save(f"{TARGET_CONTAINER}/{TARGET_FOLDER}")
)
