FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock
COPY . .
RUN pip install --no-cache-dir --no-deps .
ENV SPARK_LOCAL_IP=127.0.0.1 RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2
ENTRYPOINT ["flightledger"]
