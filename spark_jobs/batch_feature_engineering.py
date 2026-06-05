"""Spark batch job: NYC TLC raw data -> MinIO Delta Gold feature table."""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F


def make_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RideDemandBatchFeatureEngineering")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "bigdata123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_input(spark: SparkSession, path: str, fmt: str):
    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported format: {fmt}")


def engineer_labels(rides):
    cols = ["tpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance", "fare_amount", "passenger_count"]
    available = [c for c in cols if c in rides.columns]
    base = (
        rides.select(*available)
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .filter(F.col("pickup_ts").isNotNull() & F.col("PULocationID").isNotNull())
        .withColumn("hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))
        .withColumn("month", F.month("pickup_ts"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
    )
    counts = (
        base.groupBy("PULocationID", "hour", "day_of_week", "month", "is_weekend")
        .agg(
            F.count("*").alias("ride_count"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("fare_amount"), 3).alias("avg_fare_amount"),
        )
    )
    thresholds = counts.groupBy("PULocationID").agg(F.avg("ride_count").alias("location_threshold"))
    return (
        counts.join(thresholds, "PULocationID")
        .withColumn("demand_label", F.when(F.col("ride_count") > F.col("location_threshold"), "HIGH").otherwise("LOW"))
        .withColumn("label", F.when(F.col("demand_label") == "HIGH", 1.0).otherwise(0.0))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/opt/project/data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet")
    parser.add_argument("--output", default="s3a://ride-demand-lakehouse/delta/gold/ride_demand_features")
    args = parser.parse_args()

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        rides = read_input(spark, args.input, args.format)
        labelled = engineer_labels(rides)
        print("Gold feature rows:", labelled.count())
        labelled.show(10, truncate=False)
        labelled.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.output)
        print(f"Wrote Delta table to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
