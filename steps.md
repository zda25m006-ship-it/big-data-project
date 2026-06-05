# Real-Time Ride Demand Prediction System — Final Working Steps

Use **Git Bash** on Windows. Always run commands from project root:

```bash
cd /d/ride_demand_website_project/ride_demand_website_project
```

Do **not** mix `docker compose -p ride-demand ...` with `docker compose ...`. These steps use plain `docker compose ...`.

---

## 1. Clean old containers if ports conflict

```bash
docker compose down --remove-orphans
# Only if MinIO shows decodeXLHeaders error, delete old MinIO volume:
docker volume rm ride_demand_website_project_minio_data 2>/dev/null || true
```

---

## 2. Start all services

```bash
docker compose up -d --build
```

Check:

```bash
docker compose ps
```

Open:

- Website: http://localhost:8501
- API: http://localhost:3000
- MinIO: http://localhost:9001  user: `admin`, password: `bigdata123`
- MLflow: http://localhost:5000
- Spark UI: http://localhost:8081
- Grafana: http://localhost:3001 user: `admin`, password: `admin`
- Prometheus: http://localhost:9090
- Airflow: http://localhost:8080

---

## 3. Download real NYC TLC dataset

```bash
mkdir -p data/raw/nyc_tlc
curl -L -o data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet \
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
ls -lh data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet
```

---

## 4. Upload dataset to MinIO Bronze layer

```bash
bash scripts/upload_to_minio.sh
```

Open MinIO and show:

```text
ride-demand-lakehouse/bronze/yellow_taxi/yellow_tripdata_2024-01.parquet
```

---

## 5. Create Gold feature table in MinIO Delta Lake

```bash
docker compose exec spark-master bash -lc '
mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars
chmod -R 777 /tmp/.ivy2
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=bigdata123
/opt/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=bigdata123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/project/spark_jobs/batch_feature_engineering.py \
  --input /opt/project/data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet \
  --format parquet \
  --output s3a://ride-demand-lakehouse/delta/gold/ride_demand_features
'
```

Show in MinIO:

```text
ride-demand-lakehouse/delta/gold/ride_demand_features
```

---

## 6. Train Spark MLlib model using real NYC TLC data and log to MLflow

```bash
source .venv/Scripts/activate 2>/dev/null || true
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements-dev.txt boto3

mkdir -p /d/spark_tmp
export SPARK_LOCAL_DIRS="D:/spark_tmp"
export TMP="D:/spark_tmp"
export TEMP="D:/spark_tmp"
export MLFLOW_TRACKING_URI=http://localhost:5000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=bigdata123

python ml/train_spark_mllib.py \
  --input data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet \
  --format parquet \
  --model-type logistic_regression \
  --model-json-output api/model_artifacts/model.json
```

Restart API/website:

```bash
docker compose up -d --build api website
```

Test API:

```bash
curl -X POST http://localhost:3000/predict \
  -H "Content-Type: application/json" \
  -d '{"request":{"PULocationID":161,"hour":8,"day_of_week":2,"month":6,"ride_count":1.0}}'
```

Open website:

```text
http://localhost:8501
```

---

## 7. Run Kafka producer

Open a new Git Bash terminal:

```bash
cd /d/ride_demand_website_project/ride_demand_website_project
source .venv/Scripts/activate 2>/dev/null || true
python producer/kafka_producer.py \
  --input data/raw/nyc_tlc/yellow_tripdata_2024-01.parquet \
  --bootstrap localhost:9092 \
  --topic ride_events \
  --rate 5 \
  --limit 1000
```

Verify messages:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ride_events \
  --from-beginning \
  --max-messages 5
```

---

## 8. Run Spark Structured Streaming to MinIO

Open another Git Bash terminal. Keep it running.

```bash
cd /d/ride_demand_website_project/ride_demand_website_project

docker compose exec spark-master bash -lc '
mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars
chmod -R 777 /tmp/.ivy2
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=bigdata123
/opt/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=2 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=bigdata123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/project/spark_jobs/streaming_aggregation.py \
  --bootstrap kafka:29092 \
  --topic ride_events \
  --output s3a://ride-demand-lakehouse/delta/gold/streaming_ride_counts \
  --checkpoint s3a://ride-demand-lakehouse/checkpoints/streaming_ride_counts \
  --output-format delta \
  --starting-offsets earliest
'
```

In another terminal, run the producer again so streaming receives fresh data.

Check MinIO output:

```bash
bash scripts/check_minio_streaming.sh
```

If Delta streaming has trouble on Windows/WSL, use this fallback command. It still proves Kafka -> Spark Streaming -> MinIO:

```bash
docker compose exec spark-master bash -lc '
mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars
chmod -R 777 /tmp/.ivy2
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=bigdata123
/opt/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=bigdata123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/project/spark_jobs/streaming_aggregation.py \
  --bootstrap kafka:29092 \
  --topic ride_events \
  --output s3a://ride-demand-lakehouse/streaming/ride_counts_parquet \
  --checkpoint s3a://ride-demand-lakehouse/checkpoints/ride_counts_parquet \
  --output-format parquet \
  --starting-offsets earliest
'
```

---

## 9. Final demo order

1. `docker compose ps` to show all services running.
2. Open MinIO and show Bronze dataset + Gold Delta table.
3. Run producer and show Kafka messages with `kafka-console-consumer`.
4. Run Spark streaming and show MinIO streaming output/checkpoint.
5. Open MLflow and show training run.
6. Open website and make prediction.
7. Open Grafana/Prometheus.

## Final statement

"This project uses real NYC TLC Yellow Taxi Parquet data. Kafka simulates real-time taxi events. Spark Structured Streaming consumes Kafka and writes aggregate demand counts to MinIO lakehouse storage. Spark batch processing creates the Delta Gold feature table. Spark MLlib trains a HIGH/LOW ride-demand model tracked in MLflow. BentoML serves the model through an API, and Streamlit provides the web interface. Prometheus and Grafana monitor the stack."
