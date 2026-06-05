"""Spark Structured Streaming: Kafka -> Spark -> MinIO.

This version is designed for reliable demos on Windows/WSL/Docker.
It reads NYC TLC JSON events from Kafka, aggregates each micro-batch by zone/hour,
and writes the result to MinIO as either Delta or Parquet.

Use --output-format delta for the full architecture.
Use --output-format parquet as a fallback if Delta streaming is unstable on your laptop.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T


def make_spark(app_name: str = "RideDemandStreamingAggregation") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "2"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "bigdata123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="ride_events")
    parser.add_argument("--output", default="s3a://ride-demand-lakehouse/delta/gold/streaming_ride_counts")
    parser.add_argument("--checkpoint", default="s3a://ride-demand-lakehouse/checkpoints/streaming_ride_counts")
    parser.add_argument("--output-format", choices=["delta", "parquet"], default="delta")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--max-offsets-per-trigger", default="200")
    args = parser.parse_args()

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = T.StructType([
        T.StructField("event_id", T.LongType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("PULocationID", T.IntegerType(), True),
        T.StructField("DOLocationID", T.IntegerType(), True),
        T.StructField("passenger_count", T.DoubleType(), True),
        T.StructField("trip_distance", T.DoubleType(), True),
        T.StructField("fare_amount", T.DoubleType(), True),
        T.StructField("hour", T.IntegerType(), True),
        T.StructField("month", T.IntegerType(), True),
        T.StructField("day_of_week", T.IntegerType(), True),
        T.StructField("ingested_at", T.StringType(), True),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .load()
    )

    events = (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .filter(F.col("pickup_ts").isNotNull() & F.col("PULocationID").isNotNull())
        .withColumn("hour", F.coalesce(F.col("hour"), F.hour("pickup_ts")))
        .withColumn("month", F.coalesce(F.col("month"), F.month("pickup_ts")))
        .withColumn("day_of_week", F.coalesce(F.col("day_of_week"), F.dayofweek("pickup_ts")))
    )

    def write_batch(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            print(f"Batch {batch_id}: empty")
            return
        agg = (
            batch_df.groupBy("PULocationID", "hour", "day_of_week", "month")
            .agg(
                F.count("*").alias("ride_count"),
                F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
                F.round(F.avg("fare_amount"), 3).alias("avg_fare_amount"),
            )
            .withColumn("batch_id", F.lit(int(batch_id)))
            .withColumn("processed_at", F.current_timestamp())
        )
        n = agg.count()
        print(f"Batch {batch_id}: writing {n} aggregate rows to {args.output} as {args.output_format}")
        writer = agg.write.mode("append")
        if args.output_format == "delta":
            writer.format("delta").option("mergeSchema", "true").save(args.output)
        else:
            writer.format("parquet").save(args.output)

    query = (
        events.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Streaming from Kafka topic {args.topic}; output={args.output}; format={args.output_format}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
