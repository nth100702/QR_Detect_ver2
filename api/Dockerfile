FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    libzbar0 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/      ./api/
COPY core/     ./core/
COPY scanner/  ./scanner/
COPY ondemand/ ./ondemand/

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
