FROM python:3.12-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    golang-go \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

RUN git clone --depth 1 https://github.com/xitongsys/parquet-go.git /tmp/parquet-go \
    && cd /tmp/parquet-go && go build ./... \
    && git clone --depth 1 https://github.com/iwillwen/parquet-converter.git /tmp/parquet-converter \
    && cd /tmp/parquet-converter && go build -o /usr/local/bin/parquet-converter . \
    && rm -rf /tmp/parquet-go /tmp/parquet-converter


FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libgl1 \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY --from=builder /usr/local/bin/parquet-converter /usr/local/bin/parquet-converter

COPY harvester/ ./harvester/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "harvester.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
