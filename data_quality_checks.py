# ============================================================
# Great Expectations — Data Quality for NYC Taxi Silver Data
# Month 3 Week 10
#
# SETUP:
#   pip install great-expectations pandas pyarrow
#   great_expectations init   (run once in your project folder)
#
# RUN:
#   python data_quality_checks.py
# ============================================================

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from datetime import datetime
import json, io

# ── Sample silver data (replace with your actual Silver parquet) ──
SAMPLE_DATA = """trip_id,pickup_datetime,dropoff_datetime,fare_usd,tip_usd,distance_miles,passenger_count,payment_type_code,pickup_location_id
T001,2023-01-15 08:12:00,2023-01-15 08:45:00,35.50,5.00,12.1,1,1,132
T002,2023-01-15 09:05:00,2023-01-15 09:20:00,12.00,2.00,3.3,2,2,161
T003,2023-01-15 09:30:00,2023-01-15 10:05:00,28.75,4.50,8.8,1,1,138
T004,2023-01-15 10:15:00,2023-01-15 10:22:00,9.50,0.00,2.1,3,1,48
T005,2023-01-15 11:00:00,2023-01-15 11:45:00,48.00,8.00,18.2,1,1,132
T006,2023-01-15 12:30:00,2023-01-15 12:45:00,9.50,1.00,3.0,4,2,230
T007,2023-01-15 13:00:00,2023-01-15 13:35:00,34.00,0.00,11.5,2,1,132
T008,2023-01-15 14:20:00,2023-01-15 14:28:00,-7.00,0.00,1.8,1,2,68
T009,2023-01-15 15:00:00,2023-01-15 15:55:00,35.50,5.00,12.1,99,1,132
T010,2023-01-15 16:30:00,2023-01-15 16:48:00,15.00,2.50,5.4,2,1,230"""

print("=" * 60)
print("DATA QUALITY PIPELINE — NYC Taxi Silver Layer")
print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ── Load data ──────────────────────────────────────────────────
df = pd.read_csv(io.StringIO(SAMPLE_DATA))
df["pickup_datetime"]  = pd.to_datetime(df["pickup_datetime"])
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
print(f"\nLoaded {len(df)} rows, {len(df.columns)} columns")

# ── Define and run expectations ────────────────────────────────
print("\n" + "-" * 60)
print("RUNNING DATA QUALITY CHECKS")
print("-" * 60)

results = []

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    emoji  = "✓" if passed else "✗"
    print(f"  [{status}] {emoji} {name}" + (f" — {detail}" if detail else ""))
    results.append({"check": name, "status": status, "detail": detail})
    return passed

# ── Column existence checks ───────────────────────────────────
required_cols = ["trip_id","pickup_datetime","dropoff_datetime",
                 "fare_usd","distance_miles","passenger_count","payment_type_code"]
check("All required columns present",
      all(c in df.columns for c in required_cols),
      f"Required: {required_cols}")

# ── Null checks ───────────────────────────────────────────────
for col in ["trip_id", "pickup_datetime", "fare_usd"]:
    null_count = df[col].isnull().sum()
    check(f"No nulls in [{col}]", null_count == 0, f"{null_count} nulls found")

# ── Uniqueness ────────────────────────────────────────────────
dup_count = df["trip_id"].duplicated().sum()
check("trip_id is unique", dup_count == 0, f"{dup_count} duplicates found")

# ── Value range checks ────────────────────────────────────────
neg_fares = (df["fare_usd"] < 0).sum()
check("fare_usd >= 0", neg_fares == 0, f"{neg_fares} negative fares found")

high_fares = (df["fare_usd"] > 500).sum()
check("fare_usd < $500 (outlier check)", high_fares == 0, f"{high_fares} outliers found")

valid_dist = (df["distance_miles"] > 0).all()
check("distance_miles > 0", valid_dist)

valid_pax = ((df["passenger_count"] >= 1) & (df["passenger_count"] <= 6)).all()
invalid_pax = ((df["passenger_count"] < 1) | (df["passenger_count"] > 6)).sum()
check("passenger_count between 1 and 6", valid_pax, f"{invalid_pax} invalid rows")

# ── Accepted values ───────────────────────────────────────────
valid_payment_types = {1, 2, 3, 4, 5, 6}
invalid_payments = (~df["payment_type_code"].isin(valid_payment_types)).sum()
check("payment_type_code in [1,2,3,4,5,6]", invalid_payments == 0,
      f"{invalid_payments} invalid codes found")

# ── Date logic checks ─────────────────────────────────────────
bad_dates = (df["dropoff_datetime"] <= df["pickup_datetime"]).sum()
check("dropoff_datetime > pickup_datetime", bad_dates == 0,
      f"{bad_dates} rows with bad date order")

# ── Statistical checks ────────────────────────────────────────
avg_fare = df["fare_usd"].mean()
check("avg fare between $5 and $100",
      5 <= avg_fare <= 100, f"avg fare = ${avg_fare:.2f}")

# ── Completeness check ────────────────────────────────────────
total_nulls = df.isnull().sum().sum()
completeness = round((1 - total_nulls / (len(df) * len(df.columns))) * 100, 1)
check(f"Overall completeness >= 95%", completeness >= 95.0,
      f"completeness = {completeness}%")

# ── Row count check ───────────────────────────────────────────
check("Row count >= 1", len(df) >= 1, f"{len(df)} rows")

# ── Summary ───────────────────────────────────────────────────
print("\n" + "=" * 60)
print("QUALITY SUMMARY")
print("=" * 60)

passed = sum(1 for r in results if r["status"] == "PASS")
failed = sum(1 for r in results if r["status"] == "FAIL")
total  = len(results)

print(f"  Total checks:  {total}")
print(f"  Passed:        {passed}  ({passed/total*100:.0f}%)")
print(f"  Failed:        {failed}  ({failed/total*100:.0f}%)")
print(f"  Quality score: {passed/total*100:.1f}%")

# ── Separate valid vs quarantined rows ────────────────────────
print("\n" + "=" * 60)
print("DATA QUARANTINE")
print("=" * 60)

bad_rows = (
    (df["fare_usd"] < 0) |
    (df["passenger_count"] > 6) |
    (df["passenger_count"] < 1) |
    (~df["payment_type_code"].isin(valid_payment_types)) |
    (df["dropoff_datetime"] <= df["pickup_datetime"])
)

df_clean      = df[~bad_rows].copy()
df_quarantine = df[bad_rows].copy()
df_quarantine["_quarantine_reason"] = "failed quality check"

print(f"  Clean rows:       {len(df_clean)}")
print(f"  Quarantined rows: {len(df_quarantine)}")

if len(df_quarantine) > 0:
    print("\n  Quarantined records:")
    print(df_quarantine[["trip_id","fare_usd","passenger_count","payment_type_code"]].to_string(index=False))

# In production: write df_quarantine to a bad_records Delta table
# df_quarantine.to_parquet("bad_records.parquet", index=False)

print("\nData quality pipeline complete!")

# ── Final pipeline decision ───────────────────────────────────
if failed == 0:
    print("\n[DECISION] All checks passed — pipeline can proceed to Silver write.")
elif failed <= 2:
    print(f"\n[DECISION] {failed} non-critical check(s) failed — proceed with quarantine rows excluded.")
else:
    print(f"\n[DECISION] {failed} checks failed — HALT pipeline, alert data team.")
