"""Shared utility functions for ride demand label engineering and inference."""
from __future__ import annotations

import math
from typing import Dict, Iterable, List


def build_label(ride_count: float, location_threshold: float) -> int:
    """Return 1 for HIGH demand and 0 for LOW demand."""
    return int(float(ride_count) > float(location_threshold))


def sigmoid(x: float) -> float:
    """Numerically safe sigmoid."""
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def predict_probability(model: Dict, feature_values: Dict[str, float]) -> float:
    """Predict HIGH-demand probability from exported Spark Logistic Regression JSON."""
    features: List[str] = model["features"]
    coefficients: Iterable[float] = model["coefficients"]
    intercept = float(model.get("intercept", 0.0))
    score = intercept
    for name, coef in zip(features, coefficients):
        score += float(coef) * float(feature_values.get(name, 0.0))
    return sigmoid(score)


def probability_to_label(probability: float, threshold: float = 0.5) -> str:
    """Convert probability to HIGH/LOW label."""
    return "HIGH" if probability >= threshold else "LOW"
