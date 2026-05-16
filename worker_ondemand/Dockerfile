FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libgl1 libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker_ondemand/ ./worker_ondemand/
COPY worker_scheduled/ ./worker_scheduled/
COPY shared/          ./shared/

CMD ["python", "-m", "worker_ondemand.main"]