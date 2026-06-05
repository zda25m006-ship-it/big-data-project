#!/usr/bin/env bash
set -e
MSYS_NO_PATHCONV=1 docker run --rm \
  --entrypoint /bin/sh \
  --network ride_demand_website_project_default \
  minio/mc:RELEASE.2024-07-15T17-46-06Z \
  -c "mc alias set local http://minio:9000 admin bigdata123 && \
  echo '--- streaming output ---' && \
  mc ls --recursive local/ride-demand-lakehouse/delta/gold/streaming_ride_counts | head -20 || true && \
  echo '--- fallback parquet output ---' && \
  mc ls --recursive local/ride-demand-lakehouse/streaming/ride_counts_parquet | head -20 || true && \
  echo '--- checkpoint ---' && \
  mc ls --recursive local/ride-demand-lakehouse/checkpoints | head -30 || true"
