"""Memory-safe Kafka producer for NYC TLC CSV/Parquet files.

Example:
python producer/kafka_producer.py --input data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet --bootstrap localhost:9092 --topic ride_events --rate 5 --limit 1000
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Any

from kafka import KafkaProducer

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "passenger_count",
]


def clean_value(v: Any):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def row_to_event(row: Dict[str, Any], event_id: int) -> Dict[str, Any]:
    pickup_raw = row.get("tpep_pickup_datetime")
    pickup_time = clean_value(pickup_raw)
    hour = month = day_of_week = None
    try:
        dt = pickup_raw
        if hasattr(dt, "hour"):
            hour = int(dt.hour)
            month = int(dt.month)
            day_of_week = int(dt.weekday()) + 1  # Monday=1
        elif isinstance(pickup_time, str):
            dt = datetime.fromisoformat(pickup_time.replace("Z", ""))
            hour = int(dt.hour)
            month = int(dt.month)
            day_of_week = int(dt.weekday()) + 1
    except Exception:
        pass

    return {
        "event_id": int(event_id),
        "tpep_pickup_datetime": pickup_time,
        "PULocationID": int(row.get("PULocationID", 0) or 0),
        "DOLocationID": int(row.get("DOLocationID", 0) or 0),
        "trip_distance": float(row.get("trip_distance", 0.0) or 0.0),
        "fare_amount": float(row.get("fare_amount", 0.0) or 0.0),
        "passenger_count": float(row.get("passenger_count", 0.0) or 0.0),
        "hour": hour,
        "month": month,
        "day_of_week": day_of_week,
        "ingested_at": datetime.utcnow().isoformat() + "Z",
    }


def iter_parquet_rows(path: str, limit: int) -> Iterable[Dict[str, Any]]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    available = set(pf.schema.names)
    columns = [c for c in REQUIRED_COLUMNS if c in available]
    if not columns:
        raise ValueError(f"No expected taxi columns found. Available: {pf.schema.names}")

    sent = 0
    for batch in pf.iter_batches(batch_size=500, columns=columns):
        df = batch.to_pandas()
        for _, row in df.iterrows():
            sent += 1
            yield row_to_event(row.to_dict(), sent)
            if sent >= limit:
                return


def iter_csv_rows(path: str, limit: int) -> Iterable[Dict[str, Any]]:
    import pandas as pd

    sent = 0
    for chunk in pd.read_csv(path, chunksize=500):
        for _, row in chunk.iterrows():
            sent += 1
            yield row_to_event(row.to_dict(), sent)
            if sent >= limit:
                return


def iter_rows(path: str, limit: int) -> Iterable[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    if p.suffix.lower() == ".parquet":
        yield from iter_parquet_rows(str(p), limit)
    elif p.suffix.lower() == ".csv":
        yield from iter_csv_rows(str(p), limit)
    else:
        raise ValueError("Only .parquet and .csv files are supported")


def main():
    parser = argparse.ArgumentParser(description="Stream NYC TLC taxi rows into Kafka.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="ride_events")
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        retries=5,
        linger_ms=10,
    )

    delay = 1.0 / max(args.rate, 0.1)
    print("=" * 70)
    print("NYC TLC Kafka Producer")
    print("=" * 70)
    print(f"Input     : {args.input}")
    print(f"Bootstrap : {args.bootstrap}")
    print(f"Topic     : {args.topic}")
    print(f"Rate      : {args.rate} records/sec")
    print(f"Limit     : {args.limit}")
    print("=" * 70)

    count = 0
    try:
        for event in iter_rows(args.input, args.limit):
            producer.send(args.topic, key=event.get("PULocationID", "unknown"), value=event)
            count += 1
            if count <= 5 or count % 100 == 0:
                print(f"Sent {count}: {event}")
            time.sleep(delay)
        producer.flush()
        print("=" * 70)
        print(f"Finished. Total sent: {count}")
        print("=" * 70)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
