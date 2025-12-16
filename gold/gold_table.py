from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Configuration
# -----------------------------
SILVER_BASE_PATH = "abfss://silver@netflixprojectdlakegl.dfs.core.windows.net"
GOLD_BASE_PATH = "abfss://gold@netflixprojectdlakegl.dfs.core.windows.net"

# -----------------------------
# Common data quality rule
# -----------------------------
def apply_lookup_rules(df):
    """
    Apply basic data quality rules for lookup tables.
    """
    return df.filter(col("showid").isNotNull())

# -----------------------------
# Lookup Tables (Gold Layer)
# -----------------------------
lookup_tables = {
    "netflix_directors": "gold_netflix_directors",
    "netflix_cast": "gold_netflix_cast",
    "netflix_countries": "gold_netflix_countries",
    "netflix_category": "gold_netflix_categories",
}

for silver_table, gold_table in lookup_tables.items():
    df_lookup = (
        spark.readStream
        .format("delta")
        .load(f"{SILVER_BASE_PATH}/{silver_table}")
    )

    df_lookup_clean = apply_lookup_rules(df_lookup)

    (
        df_lookup_clean.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{GOLD_BASE_PATH}/_checkpoints/{gold_table}")
        .start(f"{GOLD_BASE_PATH}/{gold_table}")
    )

# -----------------------------
# Titles Dataset (Gold Layer)
# -----------------------------
df_titles = (
    spark.readStream
    .format("delta")
    .load(f"{SILVER_BASE_PATH}/netflix_titles")
)

# Add business flag
df_titles_enriched = df_titles.withColumn("newflag", lit(1))

# Apply master data quality rules
df_titles_final = (
    df_titles_enriched
    .filter(col("newflag").isNotNull())
    .filter(col("showid").isNotNull())
)

(
    df_titles_final.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{GOLD_BASE_PATH}/_checkpoints/gold_netflix_titles")
    .start(f"{GOLD_BASE_PATH}/gold_netflix_titles")
)
