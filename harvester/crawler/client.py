import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class CrawlPageData:
    url: str
    normalized_url: str
    status_code: int
    content_type: str
    html: str
    body_size: int
    metadata: dict
    links: list
    images: list
    depth: int
    markdown_content: Optional[str] = None
    downloaded_document: Optional[dict] = None
    final_url: str = ""


@dataclass
class CrawlJobResult:
    job_id: str
    status: str
    total: int = 0
    completed: int = 0
    data: list = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class MapResult:
    urls: list[dict]


@dataclass
class ScrapeResult:
    status_code: int
    content_type: str
    html: str
    body_size: int
    metadata: dict
    links: list
    images: list
    markdown_content: Optional[str] = None
    downloaded_document: Optional[dict] = None
    final_url: str = ""


class CrawlbergClient:
    """
    Client for the Crawlberg REST API server.

    Communicates with the Crawlberg API server (started via `crawlberg serve`)
    to perform discovery (map), crawling, scraping, and document downloads.
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

    async def _request(
        self,
        method: str,
        path: str,
        json: dict | None = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method=method,
                url=url,
                json=json,
                headers=self._headers(),
            )
            response.raise_for_status()
            return response.json()

    def _request_sync(
        self,
        method: str,
        path: str,
        json: dict | None = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout) as client:
            response = client.request(
                method=method,
                url=url,
                json=json,
                headers=self._headers(),
            )
            response.raise_for_status()
            return response.json()

    async def health_check(self) -> dict:
        return await self._request("GET", "/health")

    async def map_urls(
        self,
        url: str,
        limit: int | None = None,
        search: str | None = None,
    ) -> MapResult:
        """
        Discover all URLs on a website via sitemaps and link extraction.
        Synchronous operation.
        """
        payload: dict[str, Any] = {"url": url}
        if limit is not None:
            payload["limit"] = limit
        if search is not None:
            payload["search"] = search

        logger.info("Mapping URLs", url=url, limit=limit, search=search)
        response = await self._request("POST", "/v1/map", json=payload)
        data = response.get("data", {})
        return MapResult(urls=data.get("urls", []))

    async def start_crawl(
        self,
        url: str,
        max_depth: int | None = None,
        max_pages: int | None = None,
        include_paths: list[str] | None = None,
        exclude_paths: list[str] | None = None,
        only_main_content: bool = False,
    ) -> str:
        """
        Start an asynchronous crawl job. Returns a job ID for polling.
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

        logger.info("Starting crawl", url=url, max_depth=max_depth, max_pages=max_pages)
        response = await self._request("POST", "/v1/crawl", json=payload)
        job_id = response.get("id")
        if not job_id:
            raise ValueError(f"No job ID returned from crawl start: {response}")
        return job_id

    async def get_crawl_status(self, job_id: str) -> CrawlJobResult:
        """
        Poll the status of a crawl job.
        """
        response = await self._request("GET", f"/v1/crawl/{job_id}")
        pages = []
        for page in response.get("data", []) or []:
            pages.append(self._parse_crawl_page(page))
        return CrawlJobResult(
            job_id=job_id,
            status=response.get("status", "unknown"),
            total=response.get("total", 0),
            completed=response.get("completed", 0),
            data=pages,
            error=response.get("error"),
        )

    async def cancel_crawl(self, job_id: str) -> bool:
        """
        Cancel a running crawl job.
        """
        response = await self._request("DELETE", f"/v1/crawl/{job_id}")
        return response.get("success", False)

    async def scrape(self, url: str) -> ScrapeResult:
        """
        Scrape a single URL and extract content synchronously.
        """
        logger.info("Scraping URL", url=url)
        response = await self._request("POST", "/v1/scrape", json={"url": url})
        data = response.get("data", {})
        return self._parse_scrape_result(data, url)

    async def batch_scrape(self, urls: list[str]) -> str:
        """
        Start an asynchronous batch scrape job. Returns a job ID.
        """
        logger.info("Starting batch scrape", url_count=len(urls))
        response = await self._request("POST", "/v1/batch/scrape", json={"urls": urls})
        job_id = response.get("id")
        if not job_id:
            raise ValueError(f"No job ID returned from batch scrape: {response}")
        return job_id

    async def get_batch_scrape_status(self, job_id: str) -> CrawlJobResult:
        """
        Poll the status of a batch scrape job.
        """
        response = await self._request("GET", f"/v1/batch/scrape/{job_id}")
        pages = []
        for item in response.get("data", []) or []:
            if "url" in item and "error" in item:
                continue
            pages.append(self._parse_crawl_page(item))
        return CrawlJobResult(
            job_id=job_id,
            status=response.get("status", "unknown"),
            total=response.get("total", 0),
            completed=response.get("completed", 0),
            data=pages,
            error=response.get("error"),
        )

    async def download_document(self, url: str, max_size: int | None = None) -> ScrapeResult:
        """
        Download a document from a URL using the Crawlberg download endpoint.
        Returns a ScrapeResult with downloaded_document populated.
        """
        payload: dict[str, Any] = {"url": url}
        if max_size is not None:
            payload["maxSize"] = max_size

        logger.info("Downloading document", url=url, max_size=max_size)
        response = await self._request("POST", "/v1/download", json=payload)
        data = response.get("data", {})
        return self._parse_scrape_result(data, url)

    async def wait_for_crawl(
        self,
        job_id: str,
        poll_interval: int = 5,
        max_attempts: int = 720,
    ) -> CrawlJobResult:
        """
        Poll a crawl job until it completes, fails, or is cancelled.
        """
        for attempt in range(max_attempts):
            result = await self.get_crawl_status(job_id)
            logger.info(
                "Crawl status",
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

    async def start_batch_crawl(
        self,
        urls: list[str],
        max_depth: int | None = None,
        max_pages: int | None = None,
    ) -> str:
        """
        Start a batch crawl across multiple seed URLs using the Python SDK.
        Falls back to starting individual crawl jobs if the API doesn't support it.
        """
        job_ids = []
        for url in urls:
            job_id = await self.start_crawl(
                url=url,
                max_depth=max_depth,
                max_pages=max_pages,
            )
            job_ids.append(job_id)
        return job_ids[0] if len(job_ids) == 1 else job_ids

    def _parse_crawl_page(self, data: dict) -> CrawlPageData:
        markdown = data.get("markdown") or {}
        markdown_content = markdown.get("content") if isinstance(markdown, dict) else None
        return CrawlPageData(
            url=data.get("url", ""),
            normalized_url=data.get("normalized_url", data.get("url", "")),
            status_code=data.get("status_code", 0),
            content_type=data.get("content_type", ""),
            html=data.get("html", ""),
            body_size=data.get("body_size", 0),
            metadata=data.get("metadata", {}),
            links=data.get("links", []),
            images=data.get("images", []),
            depth=data.get("depth", 0),
            markdown_content=markdown_content,
            downloaded_document=data.get("downloaded_document"),
            final_url=data.get("final_url", data.get("url", "")),
        )

    def _parse_scrape_result(self, data: dict, url: str) -> ScrapeResult:
        markdown = data.get("markdown") or {}
        markdown_content = markdown.get("content") if isinstance(markdown, dict) else None
        return ScrapeResult(
            status_code=data.get("status_code", 0),
            content_type=data.get("content_type", ""),
            html=data.get("html", ""),
            body_size=data.get("body_size", 0),
            metadata=data.get("metadata", {}),
            links=data.get("links", []),
            images=data.get("images", []),
            markdown_content=markdown_content,
            downloaded_document=data.get("downloaded_document"),
            final_url=data.get("final_url", url),
        )


def get_crawlberg_client() -> CrawlbergClient:
    return CrawlbergClient()
