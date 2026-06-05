# Spark jobs

Use `batch_feature_engineering.py` for historical data and `streaming_aggregation.py` for Kafka streaming.

Recommended package set for Spark 3.5:

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_jobs/streaming_aggregation.py \
  --bootstrap localhost:9092 \
  --topic ride_events
```
