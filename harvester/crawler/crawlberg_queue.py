import asyncio
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class MapResult:
    urls: list[dict] = field(default_factory=list)


@dataclass
class CrawlJobInfo:
    job_id: str
    status: str
    total: int = 0
    completed: int = 0
    data: list = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ScrapeResult:
    url: str
    status_code: int
    content_type: str
    html: str
    body_size: int
    metadata: dict
    links: list
    images: list
    markdown: Optional[str]
    downloaded_document: Optional[dict]
    final_url: str


@dataclass
class DownloadResult:
    url: str
    filename: Optional[str]
    content_type: Optional[str]
    content: bytes
    size: int
    sha256: Optional[str]


class CrawlbergQueue:
    """
    Client for the Crawlberg API server (crawlberg serve).

    The Crawlberg API server acts as the crawl queue:
    - POST /v1/map      → Discover URLs (sitemap + link extraction)
    - POST /v1/crawl    → Submit a crawl job to the queue (returns job ID)
    - GET  /v1/crawl/{id} → Poll job status from the queue
    - DELETE /v1/crawl/{id} → Cancel a job in the queue
    - POST /v1/scrape   → Scrape a single URL synchronously
    - POST /v1/download → Download a document from a URL
    - POST /v1/batch/scrape → Submit batch scrape job to queue

    Reference: https://docs.crawlberg.xberg.io/reference/rest-api/
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: float = 300.0,
    ):
        self.base_url = (base_url or settings.crawlberg_api_url).rstrip("/")
        self.api_key = api_key or settings.crawlberg_api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _request(self, method: str, path: str, json: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method=method, url=url, json=json, headers=self._headers()
            )
            response.raise_for_status()
            return response.json()

    def _request_sync(self, method: str, path: str, json: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.request(
                method=method, url=url, json=json, headers=self._headers()
            )
            response.raise_for_status()
            return response.json()

    async def health(self) -> dict:
        return await self._request("GET", "/health")

    async def map_urls(
        self, url: str, limit: int | None = None, search: str | None = None
    ) -> MapResult:
        """
        Stage 1 - Discovery: Map all URLs on a site via sitemaps and links.
        POST /v1/map
        """
        payload: dict[str, Any] = {"url": url}
        if limit is not None:
            payload["limit"] = limit
        if search is not None:
            payload["search"] = search

        logger.info("Mapping URLs", url=url, limit=limit)
        response = await self._request("POST", "/v1/map", json=payload)
        data = response.get("data", {})
        return MapResult(urls=data.get("urls", []))

    async def enqueue_crawl(
        self,
        url: str,
        max_depth: int | None = None,
        max_pages: int | None = None,
        include_paths: list[str] | None = None,
        exclude_paths: list[str] | None = None,
        only_main_content: bool = False,
    ) -> str:
        """
        Stage 2-3 - Indexing: Submit a crawl job to Crawlberg's queue.
        Returns the job ID for polling.
        POST /v1/crawl → 202 Accepted {"success": true, "id": "<uuid>"}
        """
        payload: dict[str, Any] = {"url": url}
        if max_depth is not None:
            payload["maxDepth"] = max_depth
        if max_pages is not None:
            payload["maxPages"] = max_pages
        if include_paths:
            payload["includePaths"] = include_paths
        if exclude_paths:
            payload["excludePaths"] = exclude_paths
        if only_main_content:
            payload["onlyMainContent"] = only_main_content

        logger.info("Enqueuing crawl job", url=url, max_depth=max_depth, max_pages=max_pages)
        response = await self._request("POST", "/v1/crawl", json=payload)
        job_id = response.get("id")
        if not job_id:
            raise ValueError(f"No job ID returned: {response}")
        logger.info("Crawl job enqueued", job_id=job_id, url=url)
        return job_id

    async def get_crawl_status(self, job_id: str) -> CrawlJobInfo:
        """
        Poll the status of a crawl job in Crawlberg's queue.
        GET /v1/crawl/{id}
        """
        response = await self._request("GET", f"/v1/crawl/{job_id}")
        pages = response.get("data", []) or []
        return CrawlJobInfo(
            job_id=job_id,
            status=response.get("status", "unknown"),
            total=response.get("total", 0),
            completed=response.get("completed", 0),
            data=pages,
            error=response.get("error"),
        )

    async def cancel_crawl(self, job_id: str) -> bool:
        """
        Cancel a job in Crawlberg's queue.
        DELETE /v1/crawl/{id}
        """
        response = await self._request("DELETE", f"/v1/crawl/{job_id}")
        return response.get("success", False)

    async def scrape(self, url: str) -> ScrapeResult:
        """
        Scrape a single URL synchronously.
        POST /v1/scrape
        """
        logger.info("Scraping URL", url=url)
        response = await self._request("POST", "/v1/scrape", json={"url": url})
        return self._parse_scrape(response.get("data", {}), url)

    async def enqueue_batch_scrape(self, urls: list[str]) -> str:
        """
        Submit a batch scrape job to Crawlberg's queue.
        POST /v1/batch/scrape → 202 Accepted {"success": true, "id": "<uuid>"}
        """
        logger.info("Enqueuing batch scrape", url_count=len(urls))
        response = await self._request("POST", "/v1/batch/scrape", json={"urls": urls})
        job_id = response.get("id")
        if not job_id:
            raise ValueError(f"No job ID returned: {response}")
        return job_id

    async def get_batch_scrape_status(self, job_id: str) -> CrawlJobInfo:
        """
        Poll batch scrape job status.
        GET /v1/batch/scrape/{id}
        """
        response = await self._request("GET", f"/v1/batch/scrape/{job_id}")
        return CrawlJobInfo(
            job_id=job_id,
            status=response.get("status", "unknown"),
            total=response.get("total", 0),
            completed=response.get("completed", 0),
            data=response.get("data", []) or [],
            error=response.get("error"),
        )

    async def download(self, url: str, max_size: int | None = None) -> ScrapeResult:
        """
        Stage 4 - Extraction: Download a document via Crawlberg.
        POST /v1/download
        """
        payload: dict[str, Any] = {"url": url}
        if max_size is not None:
            payload["maxSize"] = max_size

        logger.info("Downloading document", url=url)
        response = await self._request("POST", "/v1/download", json=payload)
        return self._parse_scrape(response.get("data", {}), url)

    async def wait_for_job(
        self,
        job_id: str,
        poll_interval: int | None = None,
        max_attempts: int | None = None,
    ) -> CrawlJobInfo:
        """
        Poll a Crawlberg queue job until it completes, fails, or is cancelled.
        """
        poll_interval = poll_interval or settings.crawlberg_poll_interval
        max_attempts = max_attempts or settings.crawlberg_poll_max_attempts

        for attempt in range(max_attempts):
            result = await self.get_crawl_status(job_id)
            logger.info(
                "Crawl job status",
                job_id=job_id,
                status=result.status,
                completed=result.completed,
                total=result.total,
                attempt=attempt + 1,
            )
            if result.status in ("completed", "failed", "cancelled"):
                return result
            await asyncio.sleep(poll_interval)

        raise TimeoutError(
            f"Crawl job {job_id} did not complete within {max_attempts * poll_interval}s"
        )

    def _parse_scrape(self, data: dict, url: str) -> ScrapeResult:
        markdown = data.get("markdown") or {}
        md_content = markdown.get("content") if isinstance(markdown, dict) else None
        return ScrapeResult(
            url=url,
            status_code=data.get("status_code", 0),
            content_type=data.get("content_type", ""),
            html=data.get("html", ""),
            body_size=data.get("body_size", 0),
            metadata=data.get("metadata", {}),
            links=data.get("links", []),
            images=data.get("images", []),
            markdown=md_content,
            downloaded_document=data.get("downloaded_document"),
            final_url=data.get("final_url", url),
        )


def get_crawlberg_queue() -> CrawlbergQueue:
    return CrawlbergQueue()
