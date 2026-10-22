# Harvester — Recursive Crawling Queue Builder

An 8-stage document harvesting pipeline where **Crawlberg's API server acts as the crawl queue**. Each discovered page/record is submitted as an individual job to Crawlberg, then flows through OCR → Transform → Storage → Validation.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         HARVESTER QUEUE SYSTEM                            │
│                                                                          │
│  SEED URLS                                                               │
│     │                                                                    │
│     ▼                                                                    │
│  ┌──────────────┐    ┌──────────────────────────────────────────────┐    │
│  │  STAGE 1     │    │         CRAWLBERG API SERVER (Queue)          │    │
│  │  DISCOVERED  │───▶│  POST /v1/map  → discover all URLs            │    │
│  │              │    │                                               │    │
│  │ Crawlberg    │    │  POST /v1/crawl → enqueue job per URL         │    │
│  │ /v1/map      │    │  GET  /v1/crawl/{id} → poll job status        │    │
│  └──────────────┘    │  POST /v1/download → download documents       │    │
│                      │  POST /v1/scrape → scrape single page         │    │
│  ┌──────────────┐    │                                               │    │
│  │  STAGE 2-3   │◀───│  Each page = one QueueItem in PostgreSQL      │    │
│  │  INDEXED +   │    │  Tracked through 8 stages independently       │    │
│  │  JOB_CREATED │    └──────────────────────────────────────────────┘    │
│  └──────────────┘                                                        │
│     │                                                                    │
│     ▼                                                                    │
│  ┌──────────────┐    ┌──────────┐    ┌──────────┐    ┌────────────────┐  │
│  │  STAGE 4     │───▶│ STAGE 5  │───▶│ STAGE 6  │───▶│    STAGE 7     │  │
│  │  EXTRACTED   │    │   OCR    │    │ TRANSFORM│    │    STORED      │  │
│  │              │    │          │    │          │    │                │  │
│  │ Downloaded   │    │ Docling  │    │ parquet- │    │ R2 / MinIO     │  │
│  │ via Crawlberg│    │          │    │ converter│    │ + PostgreSQL   │  │
│  │ /v1/download │    │          │    │          │    │                │  │
│  └──────────────┘    └──────────┘    └──────────┘    └───────┬────────┘  │
│                                                              │            │
│                                                       ┌──────▼────────┐   │
│                                                       │   STAGE 8     │   │
│                                                       │   VALIDATED   │   │
│                                                       │               │   │
│                                                       │ Check all     │   │
│                                                       │ items done    │   │
│                                                       └───────────────┘   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## The 8-Stage Workflow

| Stage | Name | Tool | Crawlberg Endpoint | What Happens |
|-------|------|------|-------------------|--------------|
| 1 | **Discovered** | Crawlberg | `POST /v1/map` | Discover all URLs from seed URLs via sitemaps + link extraction |
| 2 | **Indexed** | Crawlberg | `POST /v1/crawl` | Submit crawl job to queue for each discovered URL |
| 3 | **Job Created** | Crawlberg | `GET /v1/crawl/{id}` | Crawlberg job ID stored, job runs in Crawlberg's queue |
| 4 | **Extracted** | Crawlberg | `POST /v1/download` | Poll job status, download documents, transfer to OCR |
| 5 | **OCR** | Docling | — | Extract text, markdown, structured JSON, metadata from files |
| 6 | **Transform** | parquet-converter | — | Convert Docling JSON → Apache Parquet |
| 7 | **Stored** | boto3 → R2/MinIO | — | Upload originals, parquet, JSON, markdown to object storage |
| 8 | **Validate** | PostgreSQL | — | Validate job completion, update status |

## Tools Used

| Purpose | Tool | Reference |
|---------|------|-----------|
| Discovery | Crawlberg | https://github.com/xberg-io/crawlberg |
| Extraction | Crawlberg | https://github.com/xberg-io/crawlberg |
| Proxy | Webshare | https://apidocs.webshare.io/ |
| OCR | Docling | https://github.com/docling-project/docling |
| Transform | parquet-converter | https://github.com/iwillwen/parquet-converter |
| Queue | Crawlberg API Server | https://docs.crawlberg.xberg.io/guides/api-server/ |
| REST API | Crawlberg | https://docs.crawlberg.xberg.io/reference/rest-api/ |
| Types | Crawlberg | https://docs.crawlberg.xberg.io/reference/types/ |
| Parsers | Crawlberg | https://docs.crawlberg.xberg.io/reference/parsers/ |
| Downloads | Crawlberg | https://docs.crawlberg.xberg.io/features/ |

## Quick Start

```bash
# 1. Configure
cp .env.example .env

# 2. Start all services (Crawlberg queue + PostgreSQL + Redis + MinIO + Harvester)
docker-compose up -d

# 3. Run the full pipeline with job run ID 10
python scripts/run_pipeline.py \
  --urls https://example.com https://docs.example.com \
  --job-id 10 \
  --name "Document Harvest"

# Or via API
curl -X POST http://localhost:8000/api/v1/jobs/10/run \
  -H "Content-Type: application/json" \
  -d '{
    "job_run_id": 10,
    "name": "Document Harvest",
    "seed_urls": ["https://example.com"],
    "max_depth": 3,
    "max_pages": 500
  }'
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| POST | `/api/v1/jobs` | Create a job run |
| GET | `/api/v1/jobs` | List job runs |
| GET | `/api/v1/jobs/{id}` | Get job run details |
| POST | `/api/v1/jobs/{id}/run` | Run full 8-stage pipeline |
| POST | `/api/v1/jobs/{id}/stage` | Run a single stage |
| GET | `/api/v1/jobs/{id}/items` | List queue items (pages/records) |
| GET | `/api/v1/items/{id}` | Get queue item details |
| GET | `/api/v1/items/{id}/download` | Download original file |

## Running Individual Stages

```bash
# Stage 1: Discover URLs
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "discover"}'

# Stage 2-3: Index + create crawl jobs
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "index"}'

# Stage 4: Extract (download + poll Crawlberg)
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "extract"}'

# Stage 5: OCR with Docling
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "ocr"}'

# Stage 6: Transform to Parquet
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "transform"}'

# Stage 7: Store to R2/MinIO
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "store"}'

# Stage 8: Validate
curl -X POST http://localhost:8000/api/v1/jobs/10/stage \
  -H "Content-Type: application/json" -d '{"stage": "validate"}'
```

## Services

| Service | URL | Description |
|---------|-----|-------------|
| Harvester API | http://localhost:8000 | FastAPI REST API |
| API Docs | http://localhost:8000/docs | Swagger UI |
| Crawlberg | http://localhost:3000 | Crawlberg API server (the queue) |
| MinIO Console | http://localhost:9001 | S3-compatible storage |
| PostgreSQL | localhost:5432 | Database |
| Redis | localhost:6379 | Cache/queue support |

## Configuration

See `.env.example` for all settings. Key ones:

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_JOB_RUN_ID` | `10` | Default job run ID |
| `CRAWLBERG_API_URL` | `http://localhost:3000` | Crawlberg API server |
| `WEBSHARE_USE_PROXY` | `false` | Enable Webshare proxy |
| `USE_PARQUET_CONVERTER` | `true` | Use Go parquet-converter binary |
| `R2_ACCOUNT_ID` | — | Cloudflare R2 (production) |

## License

MIT
