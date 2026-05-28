# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregations (NYC Taxi)
# MAGIC **Informatica equivalent:** Aggregator + Joiner → Target warehouse table
# MAGIC
# MAGIC This notebook builds 3 Gold tables:
# MAGIC - `gold_daily_revenue` — revenue by date
# MAGIC - `gold_zone_performance` — metrics by pickup zone
# MAGIC - `gold_hourly_demand` — trip volume by hour

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TABLE = "nyc_taxi.silver_trips"
GOLD_PATH    = "/FileStore/nyc_taxi/gold/"

# COMMAND ----------
# MAGIC %md ## Read Silver

# COMMAND ----------

df = spark.table(SILVER_TABLE)
print(f"Silver rows: {df.count():,}")

# COMMAND ----------
# MAGIC %md ## Gold Table 1 — Daily Revenue

# COMMAND ----------

gold_daily = (
    df
    .groupBy("pickup_date")
    .agg(
        F.count("*")                          .alias("trip_count"),
        F.sum("fare_amount")                  .alias("total_fare"),
        F.sum("total_amount_incl_tip")        .alias("total_revenue"),
        F.avg("fare_amount")                  .alias("avg_fare"),
        F.avg("trip_duration_mins")           .alias("avg_duration_mins"),
        F.avg("trip_distance")                .alias("avg_distance_miles"),
        F.sum(F.when(F.col("has_tip"), 1).otherwise(0)).alias("trips_with_tip"),
        F.sum("tip_amount")                   .alias("total_tips"),
    )
    .withColumn("tip_rate_pct",
        F.round(F.col("trips_with_tip") / F.col("trip_count") * 100, 1))
    .withColumn("revenue_7d_rolling",
        F.round(F.avg("total_revenue").over(
            Window.orderBy("pickup_date").rowsBetween(-6, 0)), 2))
    .withColumn("_gold_ts", F.current_timestamp())
    .orderBy("pickup_date")
)

(
    gold_daily.write.format("delta")
    .mode("overwrite")
    .option("path", GOLD_PATH + "daily_revenue/")
    .saveAsTable("nyc_taxi.gold_daily_revenue")
)

print("Gold 1 — Daily Revenue:")
gold_daily.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## Gold Table 2 — Zone Performance

# COMMAND ----------

gold_zones = (
    df
    .filter(F.col("pickup_zone_name").isNotNull())
    .groupBy("pickup_zone_name", "pickup_borough")
    .agg(
        F.count("*")                    .alias("trip_count"),
        F.sum("fare_amount")            .alias("total_revenue"),
        F.avg("fare_amount")            .alias("avg_fare"),
        F.avg("trip_distance")          .alias("avg_distance_miles"),
        F.avg("trip_duration_mins")     .alias("avg_duration_mins"),
        F.avg("passenger_count")        .alias("avg_passengers"),
    )
    .withColumn("revenue_rank",
        F.rank().over(Window.orderBy(F.col("total_revenue").desc())))
    .withColumn("_gold_ts", F.current_timestamp())
    .orderBy("revenue_rank")
)

(
    gold_zones.write.format("delta")
    .mode("overwrite")
    .option("path", GOLD_PATH + "zone_performance/")
    .saveAsTable("nyc_taxi.gold_zone_performance")
)

print("Gold 2 — Top 10 Zones by Revenue:")
gold_zones.select(
    "revenue_rank", "pickup_zone_name", "pickup_borough",
    "trip_count", "total_revenue", "avg_fare"
).show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## Gold Table 3 — Hourly Demand

# COMMAND ----------

gold_hourly = (
    df
    .groupBy("pickup_hour", "is_weekend")
    .agg(
        F.count("*")                    .alias("trip_count"),
        F.avg("fare_amount")            .alias("avg_fare"),
        F.avg("trip_duration_mins")     .alias("avg_duration_mins"),
    )
    .withColumn("period_label",
        F.when(F.col("is_weekend"), "Weekend").otherwise("Weekday"))
    .withColumn("hour_label",
        F.concat(F.lpad(F.col("pickup_hour").cast("string"), 2, "0"), F.lit(":00")))
    .withColumn("demand_tier",
        F.when(F.col("trip_count") > 5000, "peak")
         .when(F.col("trip_count") > 2000, "normal")
         .otherwise("off_peak"))
    .withColumn("_gold_ts", F.current_timestamp())
    .orderBy("pickup_hour", "is_weekend")
)

(
    gold_hourly.write.format("delta")
    .mode("overwrite")
    .option("path", GOLD_PATH + "hourly_demand/")
    .saveAsTable("nyc_taxi.gold_hourly_demand")
)

print("Gold 3 — Hourly Demand:")
gold_hourly.select(
    "hour_label", "period_label", "trip_count", "avg_fare", "demand_tier"
).show(24, truncate=False)

# COMMAND ----------
# MAGIC %md ## Summary Dashboard Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Total Revenue'   AS metric,
# MAGIC   CONCAT('$', FORMAT_NUMBER(SUM(total_revenue), 2)) AS value
# MAGIC FROM nyc_taxi.gold_daily_revenue
# MAGIC UNION ALL
# MAGIC SELECT 'Total Trips', FORMAT_NUMBER(SUM(trip_count), 0)
# MAGIC FROM nyc_taxi.gold_daily_revenue
# MAGIC UNION ALL
# MAGIC SELECT 'Avg Daily Revenue', CONCAT('$', FORMAT_NUMBER(AVG(total_revenue), 2))
# MAGIC FROM nyc_taxi.gold_daily_revenue
# MAGIC UNION ALL
# MAGIC SELECT 'Top Zone', pickup_zone_name
# MAGIC FROM nyc_taxi.gold_zone_performance
# MAGIC WHERE revenue_rank = 1
