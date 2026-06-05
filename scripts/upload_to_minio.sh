#!/usr/bin/env bash
set -e
MSYS_NO_PATHCONV=1 docker run --rm \
  --entrypoint /bin/sh \
  --network ride_demand_website_project_default \
  -v "$PWD/data/raw/nyc_tlc:/data:ro" \
  minio/mc:RELEASE.2024-07-15T17-46-06Z \
  -c "mc alias set local http://minio:9000 admin bigdata123 && \
  mc mb -p local/ride-demand-lakehouse || true && \
  mc cp /data/yellow_tripdata_2024-01.parquet local/ride-demand-lakehouse/bronze/yellow_taxi/yellow_tripdata_2024-01.parquet && \
  mc ls local/ride-demand-lakehouse/bronze/yellow_taxi/"
