FROM python:3.11-slim

WORKDIR /app

# System deps cho OpenCV + SDK
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    libzbar0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source (context là project root)
COPY api/        ./api/
COPY worker_scheduled/ ./worker_scheduled/
COPY shared/     ./shared/

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]