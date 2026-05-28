# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion (NYC Taxi)
# MAGIC **Informatica equivalent:** Source Qualifier + raw landing table
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads raw NYC Taxi Parquet from DBFS/ADLS
# MAGIC 2. Adds metadata columns
# MAGIC 3. Writes to a Bronze Delta table (idempotent)

# COMMAND ----------
# MAGIC %md ## Step 1 — Configuration

# COMMAND ----------

# ── Config — update these paths for your environment ──────────
SOURCE_PATH   = "/FileStore/nyc_taxi/raw/"         # where you uploaded the parquet file
BRONZE_TABLE  = "nyc_taxi.bronze_trips"            # Unity Catalog or hive_metastore
BRONZE_PATH   = "/FileStore/nyc_taxi/bronze/"      # Delta table storage path
CHECKPOINT    = "/FileStore/nyc_taxi/checkpoints/bronze/"
BATCH_ID      = "2023_01"                          # change per run

# COMMAND ----------
# MAGIC %md ## Step 2 — Read raw source

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Read parquet files — equivalent to Informatica Source Qualifier
df_raw = (
    spark.read
    .format("parquet")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(SOURCE_PATH)
)

print(f"Raw row count:    {df_raw.count():,}")
print(f"Raw columns:      {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------
# MAGIC %md ## Step 3 — Add metadata columns (Bronze standard pattern)

# COMMAND ----------

# Every Bronze table should have these metadata columns
# This is your audit trail — critical for debugging and lineage
df_bronze = (
    df_raw
    .withColumn("_ingestion_ts",   F.current_timestamp())
    .withColumn("_batch_id",       F.lit(BATCH_ID))
    .withColumn("_source_file",    F.input_file_name())
    .withColumn("_row_hash",       F.md5(F.concat_ws("|", *df_raw.columns)))
)

print(f"Bronze row count: {df_bronze.count():,}")
print(f"Bronze columns:   {len(df_bronze.columns)}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Write to Bronze Delta table (idempotent)

# COMMAND ----------

# MERGE makes this idempotent — safe to re-run without duplicates
# Informatica equivalent: Update Strategy with INSERT ELSE UPDATE

# First run: create the table
(
    df_bronze.write
    .format("delta")
    .mode("overwrite")                        # use "append" for incremental
    .option("overwriteSchema", "true")
    .option("path", BRONZE_PATH)
    .saveAsTable(BRONZE_TABLE)
)

print(f"Written to: {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## Step 5 — Validate & log

# COMMAND ----------

# Read back and validate
df_check = spark.table(BRONZE_TABLE)
row_count = df_check.count()
null_trip_ids = df_check.filter(F.col("VendorID").isNull()).count()

print("=" * 50)
print("BRONZE LAYER VALIDATION")
print("=" * 50)
print(f"  Total rows:       {row_count:,}")
print(f"  Null VendorIDs:   {null_trip_ids}")
print(f"  Batch ID:         {BATCH_ID}")
print(f"  Ingestion time:   {datetime.now()}")

# Log to a pipeline audit table (best practice)
audit_log = spark.createDataFrame([{
    "layer":       "bronze",
    "table":       BRONZE_TABLE,
    "batch_id":    BATCH_ID,
    "rows_written": row_count,
    "run_ts":      str(datetime.now()),
    "status":      "SUCCESS" if null_trip_ids < row_count * 0.01 else "WARNING",
}])

# Uncomment to write audit log:
# audit_log.write.format("delta").mode("append").saveAsTable("nyc_taxi.pipeline_audit")

display(df_check.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Describe history (Delta time travel)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyc_taxi.bronze_trips LIMIT 5
