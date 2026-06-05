"""Airflow DAG for the ride demand project.

This DAG demonstrates orchestration. In a full deployment, replace local python commands
with SparkSubmitOperator commands that run on your Spark cluster.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "z5008-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ride_demand_pipeline",
    description="Batch feature engineering and ML training for ride demand prediction",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 30),
    schedule="@daily",
    catchup=False,
    tags=["z5008", "ride-demand", "spark", "mlflow"],
) as dag:
    generate_sample = BashOperator(
        task_id="generate_sample_data",
        bash_command="cd /opt/project && python data/sample/generate_sample.py --rows 5000 --output data/sample/yellow_taxi_sample.csv",
    )

    train_model = BashOperator(
        task_id="train_spark_mllib_model",
        bash_command=(
            "cd /opt/project && "
            "python ml/train_spark_mllib.py "
            "--input data/sample/yellow_taxi_sample.csv "
            "--format csv "
            "--model-type logistic_regression "
            "--model-json-output api/model_artifacts/model.json"
        ),
    )

    generate_sample >> train_model
