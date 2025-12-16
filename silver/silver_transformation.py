from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, when, lit, expr, dense_rank, count
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Configuration
# -----------------------------
BRONZE_PATH = "abfss://bronze@netflixprojectdlakegl.dfs.core.windows.net/netflix_titles"
SILVER_PATH = "abfss://silver@netflixprojectdlakegl.dfs.core.windows.net/netflix_titles"

# -----------------------------
# Read Bronze Delta data
# -----------------------------
df = spark.read.format("delta").load(BRONZE_PATH)

# -----------------------------
# Data Cleaning & Type Handling
# -----------------------------
df = (
    df.withColumn(
        "duration_minutes",
        expr("coalesce(try_cast(duration_minutes as int), 0)")
    )
    .withColumn(
        "duration_seasons",
        expr("coalesce(try_cast(duration_seasons as int), 1)")
    )
)

# -----------------------------
# Feature Engineering
# -----------------------------
df = (
    df.withColumn("short_title", split(col("title"), ":")[0])
      .withColumn("rating_clean", split(col("rating"), "-")[0])
      .withColumn(
          "type_flag",
          when(col("type") == "Movie", 1)
          .when(col("type") == "TV Show", 2)
          .otherwise(0)
      )
)

# -----------------------------
# Window Function Example
# -----------------------------
window_spec = Window.orderBy(col("duration_minutes").desc())
df = df.withColumn("duration_ranking", dense_rank().over(window_spec))

# -----------------------------
# Aggregation (for analytics readiness)
# -----------------------------
df_type_summary = df.groupBy("type").agg(count("*").alias("total_count"))

# -----------------------------
# Write cleaned data to Silver layer
# -----------------------------
(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)
