# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleanse & Transform (NYC Taxi)
# MAGIC **Informatica equivalent:** Mapping with Filter + Expression + Joiner + Update Strategy
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads from Bronze Delta
# MAGIC 2. Applies data quality rules
# MAGIC 3. Derives new columns
# MAGIC 4. Joins with zone lookup
# MAGIC 5. Writes to Silver Delta using MERGE (upsert)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ── Config ─────────────────────────────────────────────────────
BRONZE_TABLE  = "nyc_taxi.bronze_trips"
SILVER_TABLE  = "nyc_taxi.silver_trips"
SILVER_PATH   = "/FileStore/nyc_taxi/silver/"
ZONE_PATH     = "/FileStore/nyc_taxi/taxi_zone_lookup.csv"   # upload this separately

# COMMAND ----------
# MAGIC %md ## Step 1 — Read Bronze

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)
print(f"Bronze row count: {df_bronze.count():,}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Data quality filter (Filter Transformation)

# COMMAND ----------

# Track counts at each stage — like Informatica session log
count_raw = df_bronze.count()

# Rule 1: Remove records with null critical fields
df_not_null = df_bronze.filter(
    F.col("tpep_pickup_datetime").isNotNull() &
    F.col("tpep_dropoff_datetime").isNotNull() &
    F.col("fare_amount").isNotNull()
)

# Rule 2: Business rules — valid trip distances and fares
df_valid = df_not_null.filter(
    (F.col("trip_distance") > 0) &
    (F.col("fare_amount") > 0) &
    (F.col("fare_amount") < 500) &           # remove outliers
    (F.col("passenger_count") > 0) &
    (F.col("passenger_count") <= 6)           # max legal passengers
)

# Rule 3: Dropoff must be after pickup
df_valid = df_valid.filter(
    F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
)

count_after_quality = df_valid.count()
rejected = count_raw - count_after_quality
print(f"Raw rows:         {count_raw:,}")
print(f"After quality:    {count_after_quality:,}")
print(f"Rejected rows:    {rejected:,}  ({rejected/count_raw*100:.1f}%)")

# COMMAND ----------
# MAGIC %md ## Step 3 — Derive columns (Expression Transformation)

# COMMAND ----------

df_silver = (
    df_valid

    # Duration in minutes
    .withColumn("trip_duration_mins",
        F.round(
            (F.col("tpep_dropoff_datetime").cast("long") -
             F.col("tpep_pickup_datetime").cast("long")) / 60,
            1
        )
    )

    # Fare per km
    .withColumn("fare_per_mile",
        F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
    )

    # Total amount including tip
    .withColumn("total_amount_incl_tip",
        F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0))
    )

    # Time-based dimensions (for Gold aggregations)
    .withColumn("pickup_date",   F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour",   F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_dow",    F.dayofweek("tpep_pickup_datetime"))   # 1=Sun, 7=Sat
    .withColumn("is_weekend",    F.dayofweek("tpep_pickup_datetime").isin([1, 7]))

    # Payment type label
    .withColumn("payment_label",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .otherwise("Unknown")
    )

    # Tip flag
    .withColumn("has_tip", F.col("tip_amount") > 0)

    # Silver metadata
    .withColumn("_silver_ts",     F.current_timestamp())
    .withColumn("_silver_batch",  F.col("_batch_id"))
)

print(f"Silver columns: {len(df_silver.columns)}")
df_silver.select(
    "tpep_pickup_datetime", "trip_distance", "fare_amount",
    "trip_duration_mins", "fare_per_mile", "pickup_hour", "payment_label"
).show(5, truncate=False)

# COMMAND ----------
# MAGIC %md ## Step 4 — Join with Zone lookup (Joiner Transformation)

# COMMAND ----------

# Load zone lookup table
# Download from: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
# Upload to Databricks FileStore, then read here

try:
    df_zones = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(ZONE_PATH)
        .select(
            F.col("LocationID"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone_name"),
        )
    )

    # Left join on pickup location
    df_silver = df_silver.join(
        df_zones,
        df_silver["PULocationID"] == df_zones["LocationID"],
        how="left"
    ).drop("LocationID")

    print(f"Zone join: {df_silver.filter(F.col('pickup_zone_name').isNotNull()).count():,} rows matched")

except Exception as e:
    print(f"Zone file not found — skipping join. Upload taxi_zone_lookup.csv first. Error: {e}")
    df_silver = df_silver.withColumn("pickup_borough", F.lit(None).cast("string"))
    df_silver = df_silver.withColumn("pickup_zone_name", F.lit(None).cast("string"))

# COMMAND ----------
# MAGIC %md ## Step 5 — Write to Silver using MERGE (Update Strategy)

# COMMAND ----------

# Create Silver table if it doesn't exist yet
if not spark.catalog.tableExists(SILVER_TABLE):
    (
        df_silver.limit(0).write
        .format("delta")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )
    print(f"Created empty Silver table: {SILVER_TABLE}")

# MERGE — Informatica Update Strategy equivalent
# If trip_id exists → UPDATE, else → INSERT
silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_delta.alias("target")
    .merge(
        df_silver.alias("source"),
        "target.VendorID = source.VendorID AND "
        "target.tpep_pickup_datetime = source.tpep_pickup_datetime AND "
        "target.PULocationID = source.PULocationID"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

final_count = spark.table(SILVER_TABLE).count()
print(f"Silver table row count: {final_count:,}")

# COMMAND ----------
# MAGIC %md ## Step 6 — Validate Silver quality

# COMMAND ----------

df_final = spark.table(SILVER_TABLE)

print("=" * 50)
print("SILVER VALIDATION")
print("=" * 50)
print(f"  Rows:             {df_final.count():,}")
print(f"  Null durations:   {df_final.filter(F.col('trip_duration_mins').isNull()).count()}")
print(f"  Negative fares:   {df_final.filter(F.col('fare_amount') < 0).count()}")
print(f"  Payment labels:")

df_final.groupBy("payment_label").count().orderBy("count", ascending=False).show()
