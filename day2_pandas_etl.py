# ============================================================
# DAY 2 — Pandas for Data Engineering
# Informatica mapping equivalents shown for every operation
# Run: pip install pandas pyarrow
# ============================================================

import pandas as pd
import io

# ── Sample data (simulates your NYC Taxi CSV) ─────────────────
RAW_CSV = """trip_id,pickup_datetime,dropoff_datetime,pickup_zone,dropoff_zone,distance_km,fare_usd,tip_usd,payment_type,passenger_count
T001,2023-01-15 08:12:00,2023-01-15 08:45:00,Airport,Downtown,22.1,35.50,5.00,card,1
T002,2023-01-15 09:05:00,2023-01-15 09:20:00,Downtown,Midtown,8.3,12.00,2.00,cash,2
T003,2023-01-15 09:30:00,2023-01-15 10:05:00,Suburbs,Airport,17.8,28.75,4.50,card,1
T004,2023-01-15 10:15:00,2023-01-15 10:22:00,Downtown,Downtown,2.1,,0.00,card,3
T005,2023-01-15 11:00:00,2023-01-15 11:45:00,Airport,Suburbs,31.2,48.00,8.00,card,1
T006,2023-01-15 12:30:00,2023-01-15 12:45:00,Midtown,Downtown,5.0,9.50,1.00,cash,4
T007,2023-01-15 13:00:00,2023-01-15 13:35:00,Downtown,Airport,21.5,34.00,0.00,card,2
T008,2023-01-15 14:20:00,2023-01-15 14:28:00,Suburbs,Suburbs,1.8,7.00,1.00,cash,1
T009,2023-01-15 15:00:00,2023-01-15 15:55:00,Airport,Downtown,22.1,35.50,5.00,card,1
T010,2023-01-15 16:30:00,2023-01-15 16:48:00,Midtown,Suburbs,9.4,15.00,2.50,card,2"""

print("=" * 60)
print("STEP 1 — Load data (Informatica: Source Qualifier)")
print("=" * 60)

df = pd.read_csv(io.StringIO(RAW_CSV))
print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
print(df.dtypes)
print(df.head(3))

# ── Type casting (Informatica: Expression Transformation) ──────
print("\n" + "=" * 60)
print("STEP 2 — Type casting & datetime parsing")
print("=" * 60)

df["pickup_datetime"]  = pd.to_datetime(df["pickup_datetime"])
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
print("Datetime columns parsed successfully")
print(df[["trip_id", "pickup_datetime", "dropoff_datetime"]].head(3))

# ── Derived columns (Informatica: Expression Transformation) ──
print("\n" + "=" * 60)
print("STEP 3 — Derived columns (Expression Transformation)")
print("=" * 60)

df["trip_duration_mins"] = (
    (df["dropoff_datetime"] - df["pickup_datetime"])
    .dt.total_seconds() / 60
).round(1)

df["total_amount"] = df["fare_usd"] + df["tip_usd"].fillna(0)
df["fare_per_km"]  = (df["fare_usd"] / df["distance_km"]).round(2)
df["pickup_hour"]  = df["pickup_datetime"].dt.hour

print(df[["trip_id", "trip_duration_mins", "total_amount", "fare_per_km", "pickup_hour"]].head(5))

# ── Null handling (Informatica: Expression with ISNULL) ────────
print("\n" + "=" * 60)
print("STEP 4 — Null handling")
print("=" * 60)

print("Nulls before:")
print(df.isnull().sum()[df.isnull().sum() > 0])

df["fare_usd"] = df["fare_usd"].fillna(df["fare_usd"].median())
df["tip_usd"]  = df["tip_usd"].fillna(0.0)

print("\nNulls after:")
print(df.isnull().sum().sum(), "total nulls remaining")

# ── Filter (Informatica: Filter Transformation) ────────────────
print("\n" + "=" * 60)
print("STEP 5 — Filter rows (Filter Transformation)")
print("=" * 60)

valid_trips   = df[df["distance_km"] > 0]
airport_trips = df[df["pickup_zone"] == "Airport"]
card_trips    = df[df["payment_type"] == "card"]

print(f"All trips:      {len(df)}")
print(f"Valid trips:    {len(valid_trips)}")
print(f"Airport trips:  {len(airport_trips)}")
print(f"Card payments:  {len(card_trips)}")

# ── Aggregation (Informatica: Aggregator Transformation) ───────
print("\n" + "=" * 60)
print("STEP 6 — Aggregation (Aggregator Transformation)")
print("=" * 60)

zone_summary = (
    df.groupby("pickup_zone")
    .agg(
        trip_count=("trip_id",    "count"),
        total_revenue=("fare_usd","sum"),
        avg_fare=("fare_usd",     "mean"),
        avg_distance=("distance_km","mean"),
    )
    .round(2)
    .reset_index()
    .sort_values("total_revenue", ascending=False)
)

print("Revenue by pickup zone:")
print(zone_summary.to_string(index=False))

# ── Sorting (Informatica: Sorter Transformation) ───────────────
print("\n" + "=" * 60)
print("STEP 7 — Sort (Sorter Transformation)")
print("=" * 60)

df_sorted = df.sort_values(["pickup_zone", "fare_usd"], ascending=[True, False])
print(df_sorted[["trip_id", "pickup_zone", "fare_usd"]].head(6).to_string(index=False))

# ── Join (Informatica: Joiner Transformation) ──────────────────
print("\n" + "=" * 60)
print("STEP 8 — Join (Joiner Transformation)")
print("=" * 60)

zone_meta = pd.DataFrame({
    "zone":     ["Airport", "Downtown", "Midtown", "Suburbs"],
    "borough":  ["Queens",  "Manhattan","Manhattan","Brooklyn"],
    "zone_type":["transport","business", "business", "residential"],
})

df_joined = df.merge(zone_meta, left_on="pickup_zone", right_on="zone", how="left")
print(df_joined[["trip_id", "pickup_zone", "borough", "zone_type"]].head(5).to_string(index=False))

# ── Write output (Informatica: Target Definition) ──────────────
print("\n" + "=" * 60)
print("STEP 9 — Write output (Target Definition)")
print("=" * 60)

df_joined.to_csv("trips_silver.csv", index=False)
df_joined.to_parquet("trips_silver.parquet", index=False)  # requires pyarrow
print("Written: trips_silver.csv")
print("Written: trips_silver.parquet")
print(f"Final row count: {len(df_joined)}")

import os
os.remove("trips_silver.csv")
os.remove("trips_silver.parquet")
print("\nAll steps complete!")
