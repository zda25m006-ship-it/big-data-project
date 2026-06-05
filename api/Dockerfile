FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py /app/service.py
COPY model_artifacts /app/model_artifacts
ENV MODEL_PATH=/app/model_artifacts/model.json
EXPOSE 3000
CMD ["bentoml", "serve", "service:RideDemandService", "--host", "0.0.0.0", "--port", "3000"]
