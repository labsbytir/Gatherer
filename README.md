# Harvester

A recursive document harvesting pipeline that crawls websites, discovers and downloads documents, performs OCR extraction, generates local embeddings, transforms data to Parquet, and stores everything in S3-compatible object storage (MinIO / Cloudflare R2).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              HARVESTER PIPELINE                              │
│                                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │  STAGE 1 │──▶│  STAGE 2 │──▶│  STAGE 3 │──▶│  STAGE 4 │──▶│  STAGE 5 │  │
│  │DISCOVERED│   │ INDEXED  │   │   JOB    │   │EXTRACTED │   │   OCR    │  │
│  │          │   │          │   │  CREATED │   │          │   │          │  │
│  │ Crawlberg│   │ Crawlberg│   │ Crawlberg│   │ Crawlberg│   │ Docling  │  │
│  │ /v1/map  │   │ /v1/crawl│   │ /v1/crawl│   │/v1/download│  │          │  │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └────┬─────┘  │
│                                                                  │         │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌───▼──────┐  │
│  │  STAGE 8 │◀──│  STAGE 7 │◀──│  STAGE 6 │◀──│  STAGE 6b│◀──│          │  │
│  │ VALIDATE │   │  STORE   │   │TRANSFORM │   │  EMBED   │   │          │  │
│  │          │   │          │   │          │   │          │   │          │  │
│  │ Postgres │   │ R2/MinIO │   │ PyArrow  │   │Local LLM │   │          │  │
│  │  check   │   │  upload  │   │ Parquet  │   │STransf.  │   │          │  │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| **Discovery** | [Crawlberg](https://github.com/xberg-io/crawlberg) `/v1/map` | Discover all URLs via sitemaps and link extraction |
| **Extraction** | [Crawlberg](https://github.com/xberg-io/crawlberg) `/v1/crawl` | Recursively crawl pages, extract HTML → Markdown, metadata, links |
| **Download** | [Crawlberg](https://github.com/xberg-io/crawlberg) `/v1/download` | Download documents (PDF, DOCX, images, etc.) |
| **Proxy** | [Webshare](https://apidocs.webshare.io/) | Rotating residential proxies for anti-bot evasion |
| **OCR** | [Docling](https://github.com/docling-project/docling) | Extract text, structure, and metadata from documents |
| **Transform** | [PyArrow](https://arrow.apache.org/docs/python/) (Parquet) | Convert Docling JSON → columnar Parquet format |
| **Embeddings** | [sentence-transformers](https://sbert.net/) | Local vector embeddings (no API calls) |
| **Storage** | [Cloudflare R2](https://developers.cloudflare.com/r2/) / MinIO | S3-compatible object storage |
| **Queue** | Redis + Celery | Async task processing with dedicated queues |
| **Database** | PostgreSQL + SQLAlchemy | Track jobs, pages, documents, and metadata |
| **API** | FastAPI | REST API for job management |

## Pipeline Stages (8-Stage Workflow)

| Stage | Name | Tool | Description |
|-------|------|------|-------------|
| 1 | **Discovered** | Crawlberg `/v1/map` | Discover all pages from seed URLs via sitemaps and links |
| 2 | **Indexed** | Crawlberg `/v1/crawl` | Recursively crawl and index each page (HTML, markdown, metadata, links) |
| 3 | **Job Created** | Crawlberg async job | Async crawl job created, polled until completion |
| 4 | **Extracted** | Crawlberg `/v1/download` | Download discovered documents (PDFs, DOCX, images) |
| 5 | **OCR** | Docling | Extract text, markdown, structured JSON, and metadata from documents |
| 6 | **Transform** | PyArrow | Convert Docling JSON → Apache Parquet (columnar format) |
| 6b | **Embed** | sentence-transformers | Generate local vector embeddings for text chunks |
| 7 | **Store** | boto3 → R2/MinIO | Upload all artifacts (original, parquet, JSON, markdown, embeddings) |
| 8 | **Validate** | PostgreSQL | Validate job completion, update status |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)

### Run with Docker Compose

```bash
# Clone the repository
git clone <repo-url>
cd Gatherer

# Copy environment variables
cp .env.example .env

# Edit .env with your settings (Webshare API key, R2 credentials, etc.)

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f api
```

### Services

| Service | URL | Description |
|---------|-----|-------------|
| API | http://localhost:8000 | FastAPI REST API |
| API Docs | http://localhost:8000/docs | Swagger UI |
| Crawlberg | http://localhost:3000 | Crawlberg REST API server |
| MinIO Console | http://localhost:9001 | S3-compatible storage (minioadmin/minioadmin) |
| PostgreSQL | localhost:5432 | Database (harvester/harvester) |
| Redis | localhost:6379 | Task queue |

### Start a Harvest Job

```bash
# Create and start a job with job run ID 10
python scripts/start_job.py \
  --name "Document Harvest #10" \
  --urls https://example.com https://docs.example.com \
  --job-id 10 \
  --max-depth 3 \
  --max-pages 500 \
  --start
```

Or via the API:

```bash
# Create a job
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Document Harvest #10",
    "seed_urls": ["https://example.com"],
    "job_run_id": 10,
    "max_depth": 3,
    "max_pages": 500
  }'

# Start the pipeline
curl -X POST http://localhost:8000/api/v1/jobs/10/start

# Check status
curl http://localhost:8000/api/v1/jobs/10

# List documents
curl http://localhost:8000/api/v1/jobs/10/documents
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check (API, Redis, Crawlberg, Storage) |
| POST | `/api/v1/jobs` | Create a new job run |
| GET | `/api/v1/jobs` | List all job runs |
| GET | `/api/v1/jobs/{id}` | Get job run details |
| POST | `/api/v1/jobs/{id}/start` | Start the 8-stage pipeline |
| GET | `/api/v1/jobs/{id}/pages` | List discovered/indexed pages |
| GET | `/api/v1/jobs/{id}/documents` | List processed documents |
| GET | `/api/v1/documents/{id}` | Get document details |
| GET | `/api/v1/documents/{id}/download` | Download original file (presigned URL) |
| POST | `/api/v1/search` | Semantic search across documents |

## Configuration

All configuration is via environment variables. See `.env.example`.

### Key Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for Celery |
| `CRAWLBERG_API_URL` | `http://localhost:3000` | Crawlberg API server URL |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO endpoint (dev) |
| `R2_ACCOUNT_ID` | - | Cloudflare R2 account ID (prod) |
| `R2_ACCESS_KEY_ID` | - | R2 access key |
| `R2_SECRET_ACCESS_KEY` | - | R2 secret key |
| `R2_BUCKET` | - | R2 bucket name |
| `WEBSHARE_API_KEY` | - | Webshare proxy API key |
| `WEBSHARE_USE_PROXY` | `false` | Enable proxy rotation |
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Local embedding model |
| `EMBEDDING_DEVICE` | `cpu` | `cpu` or `cuda` |
| `OCR_LANGUAGES` | `eng` | Tesseract/Docling OCR languages |

### Webshare Proxy Setup

1. Sign up at [webshare.io](https://www.webshare.io/) (free tier: 10 proxies)
2. Get your API key from the dashboard
3. Set in `.env`:
   ```
   WEBSHARE_USE_PROXY=true
   WEBSHARE_API_KEY=your-api-key
   WEBSHARE_PROXY_USERNAME=your-username
   WEBSHARE_PROXY_PASSWORD=your-password
   ```

### Cloudflare R2 Setup

1. Create an R2 bucket in the Cloudflare dashboard
2. Create an API token with R2 read/write permissions
3. Set in `.env`:
   ```
   R2_ACCOUNT_ID=your-account-id
   R2_ACCESS_KEY_ID=your-access-key
   R2_SECRET_ACCESS_KEY=your-secret-key
   R2_BUCKET=harvester-documents
   ```

## Development

### Local Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install system dependencies (Ubuntu/Debian)
sudo apt-get update && sudo apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libgl1 \
    libglib2.0-0

# Start infrastructure services only
docker-compose up -d postgres redis minio crawlberg

# Initialize database
python scripts/init_db.py

# Start the API server
uvicorn harvester.api.main:app --reload

# Start Celery workers (in separate terminals)
celery -A harvester.pipeline.celery_app worker -Q discovery -l INFO
celery -A harvester.pipeline.celery_app worker -Q extraction,download -l INFO
celery -A harvester.pipeline.celery_app worker -Q ocr,transform -l INFO
celery -A harvester.pipeline.celery_app worker -Q embedding,storage,validation,orchestration -l INFO
```

### Running Tests

```bash
pytest tests/ -v
```

## Data Flow

```
Seed URLs
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│  Crawlberg  │────▶│  PostgreSQL: pages table                │
│  /v1/map    │     │  (url, depth, status=discovered)        │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│  Crawlberg  │────▶│  PostgreSQL: pages table                │
│  /v1/crawl  │     │  (html, markdown, metadata, links)      │
│  (async)    │     │  + documents table (queued)             │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│  Crawlberg  │────▶│  Local filesystem: /tmp/harvester/      │
│  /v1/download│    │  (original document files)              │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│   Docling   │────▶│  PostgreSQL: documents table            │
│  (OCR+JSON) │     │  (extracted_text, markdown, docling_json)│
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│   PyArrow   │────▶│  Local: /tmp/harvester/*.parquet        │
│  (Transform)│     │  (columnar data for analytics)          │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│ sentence-   │────▶│  Local: /tmp/harvester/embeddings.json  │
│ transformers│     │  (vector embeddings per chunk)          │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│  boto3/R2   │────▶│  R2/MinIO Bucket:                       │
│  (Upload)   │     │  job-{id}/{doc_id}/original/file.pdf    │
│             │     │  job-{id}/{doc_id}/parquet/data.parquet │
│             │     │  job-{id}/{doc_id}/json/document.json   │
│             │     │  job-{id}/{doc_id}/markdown/document.md │
│             │     │  job-{id}/{doc_id}/embeddings/emb.json  │
└─────────────┘     └─────────────────────────────────────────┘
    │
    ▼
┌─────────────┐     ┌─────────────────────────────────────────┐
│  PostgreSQL │────▶│  documents.status = STORED              │
│  (Validate) │     │  job_runs.status = COMPLETED            │
└─────────────┘     └─────────────────────────────────────────┘
```

## Celery Queues

| Queue | Worker | Tasks |
|-------|--------|-------|
| `discovery` | worker-discovery | URL mapping, page discovery |
| `indexing` | worker-discovery | Crawl job creation, page indexing |
| `extraction` | worker-extraction | Page content extraction |
| `download` | worker-extraction | Document downloading |
| `ocr` | worker-ocr | Docling OCR processing |
| `transform` | worker-ocr | JSON → Parquet conversion |
| `embedding` | worker-embedding | Local embedding generation |
| `storage` | worker-embedding | R2/MinIO uploads |
| `validation` | worker-embedding | Job completion validation |
| `orchestration` | worker-embedding | Pipeline orchestration |

## License

MIT
