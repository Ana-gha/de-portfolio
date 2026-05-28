# ============================================================
# DAY 5 — Python API Pipeline
# Fetches Chennai weather from Open Meteo API (no key needed)
# Transforms it → saves as Parquet
# Run: pip install requests pyarrow pandas
# ============================================================

import requests
import pandas as pd
import json
from datetime import datetime
import os

# ── STEP 1: Fetch from API ─────────────────────────────────────
print("STEP 1 — Fetching data from Open Meteo API...")

URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=13.08&longitude=80.27"
    "&daily=temperature_2m_max,temperature_2m_min,"
    "precipitation_sum,wind_speed_10m_max"
    "&timezone=Asia/Kolkata"
    "&past_days=7"
)

try:
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"  Status: {response.status_code} OK")
    print(f"  Location: {data.get('latitude')}, {data.get('longitude')}")
except requests.exceptions.RequestException as e:
    print(f"  API call failed: {e}")
    print("  Using sample offline data instead...")
    data = {
        "daily": {
            "time":                ["2024-01-15","2024-01-16","2024-01-17","2024-01-18","2024-01-19","2024-01-20","2024-01-21"],
            "temperature_2m_max":  [32.5, 33.1, 31.8, 30.9, 32.0, 33.4, 34.1],
            "temperature_2m_min":  [22.1, 21.8, 22.5, 21.0, 21.5, 22.8, 23.0],
            "precipitation_sum":   [0.0,  0.0,  2.4,  0.0,  0.0,  0.0,  0.0],
            "wind_speed_10m_max":  [15.2, 18.4, 22.1, 14.8, 16.0, 19.3, 17.5],
        }
    }

# ── STEP 2: Parse & transform ──────────────────────────────────
print("\nSTEP 2 — Parsing and transforming...")

daily = data["daily"]
df = pd.DataFrame({
    "date":            daily["time"],
    "temp_max_c":      daily["temperature_2m_max"],
    "temp_min_c":      daily["temperature_2m_min"],
    "precipitation_mm":daily["precipitation_sum"],
    "wind_speed_kmh":  daily["wind_speed_10m_max"],
})

# Type casting
df["date"] = pd.to_datetime(df["date"])

# Derived columns
df["temp_avg_c"]      = ((df["temp_max_c"] + df["temp_min_c"]) / 2).round(1)
df["temp_range_c"]    = (df["temp_max_c"] - df["temp_min_c"]).round(1)
df["is_rainy_day"]    = df["precipitation_mm"] > 0
df["day_of_week"]     = df["date"].dt.day_name()
df["ingestion_ts"]    = datetime.now().isoformat()
df["source"]          = "open_meteo_api"
df["city"]            = "Chennai"

print(df.to_string(index=False))

# ── STEP 3: Data quality checks ───────────────────────────────
print("\nSTEP 3 — Data quality checks...")

checks = {
    "No nulls":           df.isnull().sum().sum() == 0,
    "Temp max > min":     (df["temp_max_c"] > df["temp_min_c"]).all(),
    "Precipitation >= 0": (df["precipitation_mm"] >= 0).all(),
    "Wind speed > 0":     (df["wind_speed_kmh"] > 0).all(),
    "Row count = 7-8":    6 <= len(df) <= 8,
}

all_passed = True
for check, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] {check}")
    if not passed:
        all_passed = False

print(f"\n  Overall: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")

# ── STEP 4: Save as Parquet ───────────────────────────────────
print("\nSTEP 4 — Saving as Parquet...")

output_path = "chennai_weather.parquet"
df.to_parquet(output_path, index=False)
print(f"  Saved: {output_path} ({os.path.getsize(output_path)} bytes)")

# Read back and verify
df_verify = pd.read_parquet(output_path)
print(f"  Verified: {len(df_verify)} rows read back successfully")

os.remove(output_path)
print("\nPipeline complete!")

# ── Summary ───────────────────────────────────────────────────
print("\n" + "=" * 50)
print("PIPELINE SUMMARY")
print("=" * 50)
print(f"  Source:      Open Meteo API (Chennai)")
print(f"  Rows:        {len(df)}")
print(f"  Columns:     {len(df.columns)}")
print(f"  Date range:  {df['date'].min().date()} to {df['date'].max().date()}")
print(f"  Avg temp:    {df['temp_avg_c'].mean():.1f} C")
print(f"  Rainy days:  {df['is_rainy_day'].sum()}")
