# Final Demo Script

## Opening line
"Our project is a Real-Time Ride Demand Prediction System. It streams taxi ride events through Kafka, stores processed data in MinIO Delta Lake, trains a Spark MLlib model, tracks experiments in MLflow, serves predictions through BentoML, and provides a website for real-time prediction."

## Demo flow

1. Show `docker compose ps`.
2. Open Website: http://localhost:8501.
3. Open MinIO: http://localhost:9001.
4. Open MLflow: http://localhost:5000.
5. Open Grafana: http://localhost:3001.
6. Run producer:

```bash
python producer/kafka_producer.py --input data/sample/yellow_taxi_sample.csv --bootstrap localhost:9092 --topic ride_events --rate 5 --limit 1000
```

7. Run prediction from website.
8. Run API load test:

```bash
python scripts/load_test_api.py --url http://localhost:3000/predict --requests 100 --workers 10
```

9. Conclude with model metrics and limitations.

## Limitations to mention honestly

- The website is a demo interface; Grafana is used for monitoring.
- Static TLC data is converted into a simulated stream using a Kafka producer.
- The first model is Logistic Regression for interpretability; Random Forest can be explored for non-linear patterns.
