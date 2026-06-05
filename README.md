# Real-Time Ride Demand Prediction System

**Course:** Z5008 Big Data Lab, IIT Madras Zanzibar  
**Project:** Real-Time Ride Demand Prediction System  
**Team:** Surabhi Gudla and Krishna Ryali

This repository implements an end-to-end Big Data + MLOps project:

```text
NYC TLC / sample data
      ↓
Python Kafka producer
      ↓
Apache Kafka
      ↓
Spark Structured Streaming + Spark batch jobs
      ↓
MinIO object storage + Delta Lake tables
      ↓
Spark MLlib model training
      ↓
MLflow experiment tracking
      ↓
BentoML REST API
      ↓
Streamlit website + Grafana dashboard
```

\---

## 1\. What the system predicts

The model predicts whether ride demand is **HIGH** or **LOW** for a pickup location and time.

Because the raw TLC dataset does not contain explicit demand labels, we engineer labels:

1. Aggregate ride count for each pickup location and time window.
2. Compute each pickup location's average ride count.
3. Label demand as:

   * `HIGH` if `ride\_count > location\_threshold`
   * `LOW` otherwise

\---

## 2\. Repository structure

```text
ride\_demand\_website\_project/
├── api/                    # BentoML REST API
├── website/                # Streamlit website/demo UI
├── producer/               # Kafka producer
├── spark\_jobs/             # Spark batch + streaming jobs
├── ml/                     # Spark MLlib training + MLflow logging
├── dags/                   # Airflow DAG
├── grafana/                # Grafana dashboard provisioning
├── prometheus/             # Prometheus config
├── data/sample/            # Sample generator and small demo CSV
├── scripts/                # Load test scripts
├── tests/                  # Unit tests
├── docker-compose.yml      # All major services
├── .env.example            # Environment variable template
└── README.md
```

\---

## 3\. Quick start: website + API only

Use this first to verify your website works.

```bash
cd ride\_demand\_website\_project
cp .env.example .env
python -m venv .venv
source .venv/bin/activate  # Windows Git Bash: source .venv/Scripts/activate
pip install -r requirements-dev.txt

# Terminal 1: start API
cd api
bentoml serve service:RideDemandService --host 0.0.0.0 --port 3000

# Terminal 2: start website
cd website
streamlit run app.py
```

Open:

* Website: http://localhost:8501
* API docs/endpoint: http://localhost:3000

\---

## 4\. Quick start: Docker Compose

```bash
cd ride\_demand\_website\_project
cp .env.example .env
docker compose up -d --build
```

Open services:

|Service|URL|Login|
|-|-|-|
|Website|http://localhost:8501|none|
|BentoML API|http://localhost:3000|none|
|MinIO Console|http://localhost:9001|admin / bigdata123|
|MLflow|http://localhost:5000|none|
|Grafana|http://localhost:3001|admin / admin|
|Airflow|http://localhost:8080|shown in container logs|
|Spark Master|http://localhost:8081|none|
|Prometheus|http://localhost:9090|none|

Check logs:

```bash
docker compose logs -f api
docker compose logs -f website
```

Stop everything:

```bash
docker compose down
```

\---

## 5\. Train model locally using Spark MLlib

Generate sample data:

```bash
python data/sample/generate\_sample.py --rows 5000 --output data/sample/yellow\_taxi\_sample.csv
```

Train Logistic Regression using Spark MLlib:

```bash
python ml/train\_spark\_mllib.py \\
  --input data/sample/yellow\_taxi\_sample.csv \\
  --format csv \\
  --model-type logistic\_regression \\
  --model-json-output api/model\_artifacts/model.json
```

Restart the API after training so it loads the new model.

\---

## 6\. Use real NYC TLC data

Download one or more Yellow Taxi parquet files from the NYC TLC trip record website, place them in `data/raw/`, and run:

```bash
python ml/train\_spark\_mllib.py \\
  --input data/raw/yellow\_tripdata\_2025-01.parquet \\
  --format parquet \\
  --model-type logistic\_regression \\
  --model-json-output api/model\_artifacts/model.json
```

For multiple parquet files:

```bash
python ml/train\_spark\_mllib.py \\
  --input 'data/raw/\*.parquet' \\
  --format parquet \\
  --model-type logistic\_regression \\
  --model-json-output api/model\_artifacts/model.json
```

\---

## 7\. Start Kafka streaming demo

Start all services:

```bash
docker compose up -d --build
```

Run producer from host:

```bash
python producer/kafka\_producer.py \\
  --input data/sample/yellow\_taxi\_sample.csv \\
  --bootstrap localhost:9092 \\
  --topic ride\_events \\
  --rate 5 \\
  --limit 1000
```

Or run producer as a Docker profile:

```bash
docker compose --profile stream up producer
```

\---

## 8\. Spark Structured Streaming to Delta Lake

From your host machine with Spark installed:

```bash
spark-submit \\
  --packages io.delta:delta-spark\_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.1 \\
  spark\_jobs/streaming\_aggregation.py \\
  --bootstrap localhost:9092 \\
  --topic ride\_events \\
  --output s3a://ride-demand-lakehouse/delta/streaming\_ride\_counts \\
  --checkpoint s3a://ride-demand-lakehouse/checkpoints/streaming\_ride\_counts
```

For Docker Spark master, execute inside the Spark master container:

```bash
docker compose exec spark-master bash
cd /opt/project
spark-submit \\
  --master spark://spark-master:7077 \\
  --packages io.delta:delta-spark\_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.1 \\
  spark\_jobs/streaming\_aggregation.py \\
  --bootstrap kafka:29092 \\
  --topic ride\_events \\
  --output s3a://ride-demand-lakehouse/delta/streaming\_ride\_counts \\
  --checkpoint s3a://ride-demand-lakehouse/checkpoints/streaming\_ride\_counts
```

\---

## 9\. MLflow experiment tracking

With Docker Compose running, set:

```bash
export MLFLOW\_TRACKING\_URI=http://localhost:5000
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv
```

Open MLflow at http://localhost:5000 and show your experiment runs.

For final demo, run at least 5 experiments:

```bash
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv --model-type logistic\_regression
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv --model-type logistic\_regression
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv --model-type logistic\_regression
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv --model-type logistic\_regression
python ml/train\_spark\_mllib.py --input data/sample/yellow\_taxi\_sample.csv --format csv --model-type logistic\_regression
```

You can modify regularization, features, or date ranges before each run.

\---

## 10\. Load test for final demo

The course asks you to show system stability under 10× load. Run:

```bash
python scripts/load\_test\_api.py --url http://localhost:3000/predict --requests 100 --workers 10
```

Show the output and Grafana dashboard during the demo.

\---

## 11\. Unit tests

```bash
pytest -q
```

\---

## 12\. Final live demo order

1. `docker compose up -d --build`
2. Open MinIO, MLflow, Website, Grafana.
3. Run Kafka producer.
4. Run Spark streaming aggregation.
5. Show data appearing in MinIO Delta path.
6. Run Spark MLlib training and show MLflow run.
7. Restart API if model changed.
8. Use website to predict demand.
9. Run load test.
10. Show Grafana panels.

\---

