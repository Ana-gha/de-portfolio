# ============================================================
# DAY 1 — Python Basics for Data Engineering
# Run this file section by section in VS Code or Jupyter
# ============================================================

# ── 1. Lists ──────────────────────────────────────────────────
print("=" * 50)
print("LISTS")
print("=" * 50)

trips = [12.5, 8.3, 22.1, 5.0, 17.8, 9.4, 31.2]

print(f"All trips:        {trips}")
print(f"First trip:       {trips[0]}")
print(f"Last trip:        {trips[-1]}")
print(f"Total trips:      {len(trips)}")
print(f"Total distance:   {sum(trips):.1f} km")
print(f"Average distance: {sum(trips)/len(trips):.1f} km")
print(f"Max distance:     {max(trips)} km")
print(f"Min distance:     {min(trips)} km")

# Filter trips longer than 10 km  (like Informatica Filter Transformation)
long_trips = [t for t in trips if t > 10]
print(f"Long trips (>10): {long_trips}")

# ── 2. Dictionaries ───────────────────────────────────────────
print("\n" + "=" * 50)
print("DICTIONARIES")
print("=" * 50)

trip_record = {
    "trip_id":        "T001",
    "pickup_zone":    "Airport",
    "dropoff_zone":   "Downtown",
    "distance_km":    22.1,
    "fare_usd":       35.50,
    "payment_type":   "card",
    "passenger_count": 2,
}

print(f"Trip ID:      {trip_record['trip_id']}")
print(f"Route:        {trip_record['pickup_zone']} → {trip_record['dropoff_zone']}")
print(f"Fare:         ${trip_record['fare_usd']}")

# Add a new field (like Expression Transformation in Informatica)
trip_record["fare_per_km"] = round(trip_record["fare_usd"] / trip_record["distance_km"], 2)
print(f"Fare per km:  ${trip_record['fare_per_km']}")

# Loop through all fields
print("\nAll fields:")
for key, value in trip_record.items():
    print(f"  {key:20s}: {value}")

# ── 3. Tuples & Sets ──────────────────────────────────────────
print("\n" + "=" * 50)
print("TUPLES & SETS")
print("=" * 50)

# Tuples — immutable (use for fixed config values)
db_config = ("adb-xxxxx.azuredatabricks.net", 443, "sql/protocolv1/o/xxxxx")
host, port, path = db_config  # unpacking
print(f"Host: {host}, Port: {port}")

# Sets — unique values (great for deduplication checks)
payment_types_raw = ["card", "cash", "card", "card", "cash", "unknown", "card"]
unique_payments = set(payment_types_raw)
print(f"Raw payments:    {payment_types_raw}")
print(f"Unique payments: {unique_payments}")
print(f"Duplicates found: {len(payment_types_raw) - len(unique_payments)}")

# ── 4. Control Flow ───────────────────────────────────────────
print("\n" + "=" * 50)
print("CONTROL FLOW")
print("=" * 50)

def classify_trip(distance_km):
    """Classify trip length — like a Router Transformation in Informatica."""
    if distance_km < 5:
        return "short"
    elif distance_km < 15:
        return "medium"
    else:
        return "long"

for trip in trips:
    category = classify_trip(trip)
    print(f"  {trip:5.1f} km → {category}")

# ── 5. Error Handling ─────────────────────────────────────────
print("\n" + "=" * 50)
print("ERROR HANDLING")
print("=" * 50)

raw_fares = ["12.50", "N/A", "8.30", "", "22.10", "unknown"]

clean_fares = []
rejected_fares = []

for raw in raw_fares:
    try:
        fare = float(raw)
        clean_fares.append(fare)
    except ValueError:
        rejected_fares.append(raw)
        print(f"  [REJECTED] Could not parse fare: '{raw}'")

print(f"\nClean fares:    {clean_fares}")
print(f"Rejected count: {len(rejected_fares)}")
print(f"Rejection rate: {len(rejected_fares)/len(raw_fares)*100:.1f}%")

# ── 6. List Comprehensions ────────────────────────────────────
print("\n" + "=" * 50)
print("LIST COMPREHENSIONS")
print("=" * 50)

fares = [10.5, 22.0, 5.5, 18.75, 12.0, 30.0]

# Without list comprehension
fares_with_tax_loop = []
for f in fares:
    fares_with_tax_loop.append(round(f * 1.18, 2))

# With list comprehension (much cleaner — use this in DE work)
fares_with_tax = [round(f * 1.18, 2) for f in fares]
fares_over_15   = [f for f in fares if f > 15]

print(f"Original fares:   {fares}")
print(f"With 18% tax:     {fares_with_tax}")
print(f"Fares over $15:   {fares_over_15}")

# ── 7. File I/O ───────────────────────────────────────────────
print("\n" + "=" * 50)
print("FILE I/O — Read & Write CSV")
print("=" * 50)

import csv
import os

# Write a sample CSV
sample_data = [
    {"trip_id": "T001", "zone": "Airport", "fare": 35.50, "distance": 22.1},
    {"trip_id": "T002", "zone": "Downtown", "fare": 12.00, "distance": 8.3},
    {"trip_id": "T003", "zone": "Suburbs", "fare": 28.75, "distance": 17.8},
]

csv_path = "sample_trips.csv"
with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["trip_id", "zone", "fare", "distance"])
    writer.writeheader()
    writer.writerows(sample_data)

print(f"Written {len(sample_data)} rows to {csv_path}")

# Read it back
with open(csv_path, "r") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"Read back {len(rows)} rows:")
for row in rows:
    print(f"  {row['trip_id']}: {row['zone']} — ${row['fare']}")

# Clean up
os.remove(csv_path)
print("\nDone! All exercises complete.")
