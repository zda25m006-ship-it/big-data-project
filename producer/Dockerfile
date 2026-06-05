FROM python:3.11-slim
WORKDIR /app
COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/kafka_producer.py /app/kafka_producer.py
COPY data/sample/yellow_taxi_sample.csv /app/data/sample/yellow_taxi_sample.csv
CMD ["python", "kafka_producer.py", "--input", "data/sample/yellow_taxi_sample.csv", "--bootstrap", "kafka:29092", "--topic", "ride_events", "--rate", "5"]
