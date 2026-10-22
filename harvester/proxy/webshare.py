from dataclasses import dataclass
from typing import Optional

import httpx

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

WEBSHARE_API_BASE = "https://proxy.webshare.io/api/v2"


@dataclass
class ProxyConfig:
    url: str
    username: Optional[str] = None
    password: Optional[str] = None


class WebshareProxy:
    """
    Webshare proxy manager for Crawlberg requests.

    Supports:
    - Rotating proxy endpoint (each request gets a different IP)
    - Proxy list from Webshare API
    - Sticky sessions

    Reference: https://apidocs.webshare.io/
    """

    def __init__(
        self,
        api_key: str | None = None,
        username: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        enabled: bool | None = None,
    ):
        self.api_key = api_key or settings.webshare_api_key
        self.username = username or settings.webshare_proxy_username
        self.password = password or settings.webshare_proxy_password
        self.host = host or settings.webshare_proxy_host
        self.port = port or settings.webshare_proxy_port
        self.enabled = enabled if enabled is not None else settings.webshare_use_proxy
        self._proxy_list: list[dict] = []

    @property
    def rotating_endpoint(self) -> str:
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"

    def get_proxy_config(self) -> ProxyConfig | None:
        if not self.enabled:
            return None
        return ProxyConfig(
            url=self.rotating_endpoint,
            username=self.username,
            password=self.password,
        )

    def get_crawlberg_proxy(self) -> dict | None:
        """
        Proxy config in Crawlberg ProxyConfig format:
        {"url": "http://user:pass@host:port", "username": "...", "password": "..."}
        """
        if not self.enabled:
            return None
        cfg = {"url": self.rotating_endpoint}
        if self.username:
            cfg["username"] = self.username
        if self.password:
            cfg["password"] = self.password
        return cfg

    def get_env_proxies(self) -> dict[str, str] | None:
        if not self.enabled:
            return None
        url = self.rotating_endpoint
        return {
            "HTTP_PROXY": url,
            "HTTPS_PROXY": url,
            "http_proxy": url,
            "https_proxy": url,
        }

    async def fetch_proxy_list(self, page: int = 1, page_size: int = 100) -> list[dict]:
        if not self.api_key:
            raise ValueError("Webshare API key required")
        url = f"{WEBSHARE_API_BASE}/proxy/list/"
        headers = {"Authorization": f"Token {self.api_key}"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url, headers=headers, params={"page": page, "page_size": page_size}
            )
            resp.raise_for_status()
            self._proxy_list = resp.json().get("results", [])
            return self._proxy_list

    async def refresh_proxies(self) -> dict:
        if not self.api_key:
            raise ValueError("Webshare API key required")
        url = f"{WEBSHARE_API_BASE}/proxy/list/refresh/"
        headers = {"Authorization": f"Token {self.api_key}"}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=headers)
            resp.raise_for_status()
            return resp.json()


_proxy: WebshareProxy | None = None


def get_proxy() -> WebshareProxy:
    global _proxy
    if _proxy is None:
        _proxy = WebshareProxy()
    return _proxy
