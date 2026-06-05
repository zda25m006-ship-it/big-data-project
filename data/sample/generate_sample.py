"""Generate a small synthetic taxi-like sample dataset for local testing.
This lets you run the full demo before downloading NYC TLC parquet files.
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def generate_rows(n: int = 5000):
    random.seed(42)
    start = datetime(2026, 1, 1, 0, 0, 0)
    hot_locations = [68, 79, 132, 138, 161, 162, 230, 236, 237]
    all_locations = list(range(1, 264))
    rows = []
    for i in range(n):
        hour_offset = random.randint(0, 24 * 28)
        pickup_time = start + timedelta(hours=hour_offset, minutes=random.randint(0, 59))
        rush = pickup_time.hour in [7, 8, 9, 17, 18, 19, 20]
        weekend = pickup_time.weekday() >= 5
        if rush or weekend:
            pu = random.choice(hot_locations + random.sample(all_locations, 10))
        else:
            pu = random.choice(all_locations)
        duration = random.randint(6, 55)
        do = random.choice(all_locations)
        trip_distance = max(0.3, random.gauss(3.2 if rush else 2.1, 1.2))
        fare = 3.0 + trip_distance * 2.8 + duration * 0.25
        rows.append(
            {
                "VendorID": random.choice([1, 2]),
                "tpep_pickup_datetime": pickup_time.isoformat(sep=" "),
                "tpep_dropoff_datetime": (pickup_time + timedelta(minutes=duration)).isoformat(sep=" "),
                "passenger_count": random.choice([1, 1, 1, 2, 2, 3, 4]),
                "trip_distance": round(trip_distance, 2),
                "PULocationID": pu,
                "DOLocationID": do,
                "fare_amount": round(fare, 2),
            }
        )
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--output", default="data/sample/yellow_taxi_sample.csv")
    args = parser.parse_args()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = generate_rows(args.rows)
    df.to_csv(out, index=False)
    print(f"Saved {len(df):,} rows to {out}")


if __name__ == "__main__":
    main()
