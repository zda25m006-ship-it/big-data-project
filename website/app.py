"""Streamlit website for the Real-Time Ride Demand Prediction System."""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:3000").rstrip("/")

st.set_page_config(page_title="Ride Demand Prediction", page_icon="🚕", layout="wide")
st.title("🚕 Real-Time Ride Demand Prediction System")
st.caption("Kafka → MinIO/Delta Lake → Spark → MLflow → BentoML API → Website + Grafana")

with st.sidebar:
    st.header("System")
    st.write(f"API URL: `{API_URL}`")
    if st.button("Check API health"):
        try:
            r = requests.post(f"{API_URL}/health", json={}, timeout=5)
            st.success("API is running")
            st.json(r.json())
        except Exception as exc:
            st.error(f"API not reachable: {exc}")

left, right = st.columns([1, 1])

with left:
    st.subheader("Make a demand prediction")
    with st.form("predict_form"):
        c1, c2 = st.columns(2)
        with c1:
            pu_location = st.number_input("Pickup Location ID", min_value=1, max_value=263, value=161, step=1)
            hour = st.slider("Hour of day", min_value=0, max_value=23, value=datetime.now().hour)
        with c2:
            day_of_week = st.selectbox(
                "Day of week (Spark: 1=Sunday)",
                options=[1, 2, 3, 4, 5, 6, 7],
                format_func=lambda x: {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 5: "Thursday", 6: "Friday", 7: "Saturday"}[x],
                index=2,
            )
            month = st.slider("Month", min_value=1, max_value=12, value=datetime.now().month)
        ride_count = st.number_input("Optional current aggregated ride count", min_value=0.0, value=0.0, step=1.0)
        submitted = st.form_submit_button("Predict demand")

    if submitted:
        payload = {
            "PULocationID": int(pu_location),
            "hour": int(hour),
            "day_of_week": int(day_of_week),
            "month": int(month),
            "ride_count": float(ride_count),
        }
        try:
            # BentoML expects the parameter name around the payload: {"request": {...}}
            response = requests.post(f"{API_URL}/predict", json={"request": payload}, timeout=10)
            if response.status_code == 400:
                st.warning("API returned 400. Showing server message below.")
                st.code(response.text)
            response.raise_for_status()
            result = response.json()
            if result["demand"] == "HIGH":
                st.error(f"HIGH demand predicted — probability {result['probability_high']}")
            else:
                st.success(f"LOW demand predicted — probability {result['probability_high']}")
            st.info(result["recommendation"])
            st.json(result)
        except Exception as exc:
            st.error(f"Prediction failed: {exc}")
            st.write("Check API: `curl -X POST http://localhost:3000/health -H 'Content-Type: application/json' -d '{}'`")

with right:
    st.subheader("Demo explanation")
    st.markdown(
        """
        This website is your **live demo interface**. It calls the BentoML API that serves the Spark MLlib model.

        **Final demo proof order:**
        1. Open MinIO and show `ride-demand-lakehouse` bucket.
        2. Run Kafka producer and show JSON records in the Kafka topic.
        3. Run Spark streaming and show MinIO output/checkpoint files.
        4. Open MLflow and show the training run.
        5. Use this website for real-time prediction.
        6. Open Grafana/Prometheus for monitoring.
        """
    )

    sample = pd.DataFrame([
        {"PULocationID": 161, "hour": 8, "day_of_week": 2, "month": 6, "Expected": "Likely high"},
        {"PULocationID": 236, "hour": 18, "day_of_week": 5, "month": 6, "Expected": "Likely high"},
        {"PULocationID": 13, "hour": 3, "day_of_week": 2, "month": 6, "Expected": "Likely low"},
    ])
    st.dataframe(sample, use_container_width=True)

st.divider()
st.subheader("Architecture")
st.code(
    "NYC TLC data → Kafka Producer → Kafka topic → Spark Structured Streaming → MinIO Delta/Parquet Lakehouse\n"
    "Delta/Lakehouse → Spark MLlib Training → MLflow tracking → BentoML API → Streamlit Website\n"
    "Prometheus/Grafana monitor API and system metrics",
    language="text",
)
