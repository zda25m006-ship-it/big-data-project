"""Train Spark MLlib Logistic Regression for HIGH/LOW ride demand.

Input can be a CSV or parquet file with at least:
- tpep_pickup_datetime
- PULocationID

Label engineering:
1. Aggregate rides per PULocationID + hour + day_of_week + month.
2. Compute each location's average ride_count.
3. HIGH demand = ride_count > location average; otherwise LOW.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions as F


def make_spark(app_name: str = "RideDemandTraining") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def read_rides(spark: SparkSession, path: str, input_format: str):
    if input_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if input_format == "parquet":
        return spark.read.parquet(path)
    if input_format == "delta":
        return spark.read.format("delta").load(path)
    raise ValueError(f"Unsupported input format: {input_format}")


def build_training_table(rides):
    df = (
        rides.select("tpep_pickup_datetime", "PULocationID")
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("PULocationID").isNotNull())
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .filter(F.col("pickup_ts").isNotNull())
        .withColumn("hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))
        .withColumn("month", F.month("pickup_ts"))
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn("PULocationID", F.col("PULocationID").cast("double"))
    )

    counts = (
        df.groupBy("PULocationID", "hour", "day_of_week", "month", "is_weekend")
        .agg(F.count("*").alias("ride_count"))
    )

    thresholds = counts.groupBy("PULocationID").agg(F.avg("ride_count").alias("location_threshold"))

    train_df = (
        counts.join(thresholds, on="PULocationID", how="inner")
        .withColumn("label", F.when(F.col("ride_count") > F.col("location_threshold"), 1.0).otherwise(0.0))
        .select("PULocationID", "hour", "day_of_week", "month", "is_weekend", "ride_count", "location_threshold", "label")
    )
    return train_df


def evaluate(predictions) -> dict:
    metrics = {}
    for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric_name
        )
        metrics[metric_name] = float(evaluator.evaluate(predictions))

    try:
        auc_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        metrics["auc"] = float(auc_eval.evaluate(predictions))
    except Exception:
        metrics["auc"] = None
    return metrics


def train_model(train_df, model_type: str = "logistic_regression"):
    feature_cols = ["PULocationID", "hour", "day_of_week", "month", "is_weekend"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    if model_type == "random_forest":
        clf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=80, maxDepth=8, seed=42)
    else:
        clf = LogisticRegression(labelCol="label", featuresCol="features", maxIter=80, regParam=0.03, elasticNetParam=0.0)

    pipeline = Pipeline(stages=[assembler, clf])
    train, test = train_df.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train)
    predictions = model.transform(test)
    metrics = evaluate(predictions)
    return model, metrics, feature_cols, train_df.count()


def export_lightweight_lr_model(model, metrics: dict, feature_cols: list[str], output_path: str, training_rows: int):
    """Export a lightweight JSON model for BentoML inference.

    For RandomForest, we do not export tree internals here. Use logistic_regression
    for the website/API demo because it is reliable and lightweight.
    """
    last_stage = model.stages[-1]
    if not hasattr(last_stage, "coefficients"):
        raise ValueError("JSON export currently supports LogisticRegression only. Use --model-type logistic_regression.")

    payload = {
        "model_type": "spark_mllib_logistic_regression_export",
        "features": feature_cols,
        "coefficients": [float(x) for x in last_stage.coefficients.toArray().tolist()],
        "intercept": float(last_stage.intercept),
        "threshold": 0.5,
        "metrics": metrics,
        "training_rows": int(training_rows),
    }
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def log_to_mlflow(metrics: dict, model_type: str, model_json_path: str):
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        print("MLFLOW_TRACKING_URI not set; skipping MLflow logging.")
        return
    try:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("ride-demand-prediction")
        with mlflow.start_run(run_name=model_type):
            mlflow.log_param("model_type", model_type)
            for k, v in metrics.items():
                if v is not None:
                    mlflow.log_metric(k, float(v))
            mlflow.log_artifact(model_json_path, artifact_path="model_export")
        print(f"Logged run to MLflow: {tracking_uri}")
    except Exception as exc:
        print(f"MLflow logging skipped due to error: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/sample/yellow_taxi_sample.csv")
    parser.add_argument("--format", choices=["csv", "parquet", "delta"], default="csv")
    parser.add_argument("--model-type", choices=["logistic_regression", "random_forest"], default="logistic_regression")
    parser.add_argument("--model-json-output", default="api/model_artifacts/model.json")
    parser.add_argument("--metrics-output", default="ml/artifacts/metrics.json")
    args = parser.parse_args()

    spark = make_spark()
    try:
        rides = read_rides(spark, args.input, args.format)
        train_df = build_training_table(rides).cache()
        print("Training rows:", train_df.count())
        print("Label distribution:")
        train_df.groupBy("label").count().show()

        model, metrics, features, training_rows = train_model(train_df, args.model_type)
        print("Metrics:", metrics)

        if args.model_type == "logistic_regression":
            export_lightweight_lr_model(model, metrics, features, args.model_json_output, training_rows)
            Path(args.metrics_output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.metrics_output).write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            log_to_mlflow(metrics, args.model_type, args.model_json_output)
            print(f"Saved lightweight model to {args.model_json_output}")
        else:
            print("RandomForest trained/evaluated. Use logistic_regression for lightweight BentoML export.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
