.PHONY: setup up down api web train producer test load-test

setup:
	cp -n .env.example .env || true
	python -m pip install -r requirements-dev.txt
	python data/sample/generate_sample.py --rows 5000 --output data/sample/yellow_taxi_sample.csv

up:
	docker compose up -d --build

down:
	docker compose down

api:
	cd api && bentoml serve service:RideDemandService --host 0.0.0.0 --port 3000

web:
	cd website && streamlit run app.py

train:
	python ml/train_spark_mllib.py --input data/sample/yellow_taxi_sample.csv --format csv --model-type logistic_regression --model-json-output api/model_artifacts/model.json

producer:
	python producer/kafka_producer.py --input data/sample/yellow_taxi_sample.csv --bootstrap localhost:9092 --topic ride_events --rate 5 --limit 1000

test:
	pytest -q

load-test:
	python scripts/load_test_api.py --url http://localhost:3000/predict --requests 100
