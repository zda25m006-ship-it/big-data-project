"""BentoML REST API for real-time ride demand prediction."""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import bentoml
from pydantic import BaseModel, Field


def sigmoid(x: float) -> float:
    import math

    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


class RideRequest(BaseModel):
    PULocationID: int = Field(..., ge=1, le=263, description="NYC TLC pickup location ID")
    hour: int = Field(..., ge=0, le=23, description="Hour of day")
    day_of_week: int = Field(..., ge=1, le=7, description="Spark dayofweek: 1=Sunday, 7=Saturday")
    month: int = Field(..., ge=1, le=12)
    ride_count: Optional[float] = Field(None, ge=0, description="Optional current aggregated ride count")


@bentoml.service(resources={"cpu": "1"}, traffic={"timeout": 20})
class RideDemandService:
    def __init__(self) -> None:
        model_path = Path(os.getenv("MODEL_PATH", "/app/model_artifacts/model.json"))
        if not model_path.exists():
            model_path = Path(__file__).parent / "model_artifacts" / "model.json"
        self.model_path = model_path
        self.model = json.loads(model_path.read_text(encoding="utf-8"))

    @bentoml.api(route="/health")
    def health(self) -> dict:
        return {
            "status": "ok",
            "service": "ride-demand-bentoml-api",
            "model_path": str(self.model_path),
            "model_type": self.model.get("model_type"),
            "training_rows": self.model.get("training_rows", 0),
            "metrics": self.model.get("metrics", {}),
            "time": datetime.utcnow().isoformat() + "Z",
        }

    @bentoml.api(route="/predict")
    def predict(self, request: RideRequest) -> dict:
        is_weekend = 1.0 if request.day_of_week in [1, 7] else 0.0
        feature_values = {
            "PULocationID": float(request.PULocationID),
            "hour": float(request.hour),
            "day_of_week": float(request.day_of_week),
            "month": float(request.month),
            "is_weekend": is_weekend,
        }

        score = float(self.model.get("intercept", 0.0))
        for name, coef in zip(self.model["features"], self.model["coefficients"]):
            score += float(coef) * float(feature_values.get(name, 0.0))

        probability_high = sigmoid(score)
        threshold = float(self.model.get("threshold", 0.5))
        demand = "HIGH" if probability_high >= threshold else "LOW"
        recommendation = (
            "Move more drivers toward this pickup zone."
            if demand == "HIGH"
            else "Normal driver allocation is enough for this zone."
        )

        return {
            "demand": demand,
            "probability_high": round(probability_high, 4),
            "threshold": threshold,
            "features_used": feature_values,
            "recommendation": recommendation,
        }
