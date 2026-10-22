import random
from dataclasses import dataclass
from typing import Optional

import httpx

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class ProxyConfig:
    url: str
    username: Optional[str] = None
    password: Optional[str] = None

    @property
    def proxy_dict(self) -> dict[str, str]:
        return {"http://": self.url, "https://": self.url}


class ProxyManager:
    """
    Manages Webshare proxy connections for the crawler.

    Supports:
    - Rotating proxy endpoint (single endpoint, rotates per request)
    - Direct proxy list (fetched from Webshare API)
    - Sticky sessions
    """

    WEBSHARE_API_BASE = "https://proxy.webshare.io/api/v2"

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
        self._current_index: int = 0

    @property
    def rotating_proxy_url(self) -> str:
        """
        Build the rotating proxy endpoint URL.
        Each request through this endpoint gets a different IP.
        """
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"

    def get_proxy_config(self) -> ProxyConfig | None:
        """
        Get the current proxy configuration for use with HTTP clients.
        Returns None if proxy is disabled.
        """
        if not self.enabled:
            return None
        return ProxyConfig(
            url=self.rotating_proxy_url,
            username=self.username,
            password=self.password,
        )

    def get_httpx_proxies(self) -> dict[str, str] | None:
        """
        Get proxies dict for httpx client.
        """
        if not self.enabled:
            return None
        url = self.rotating_proxy_url
        return {"http://": url, "https://": url, "all://": url}

    async def fetch_proxy_list(self, page: int = 1, page_size: int = 25) -> list[dict]:
        """
        Fetch the list of available proxies from the Webshare API.
        """
        if not self.api_key:
            raise ValueError("Webshare API key is required to fetch proxy list")

        url = f"{self.WEBSHARE_API_BASE}/proxy/list/"
        headers = {"Authorization": f"Token {self.api_key}"}
        params = {"page": page, "page_size": page_size, "mode": "rotating"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            self._proxy_list = data.get("results", [])
            return self._proxy_list

    def get_next_proxy(self) -> ProxyConfig | None:
        """
        Get the next proxy from the list (round-robin).
        Falls back to the rotating endpoint if no list is loaded.
        """
        if not self.enabled:
            return None

        if self._proxy_list:
            proxy = self._proxy_list[self._current_index % len(self._proxy_list)]
            self._current_index += 1
            host = proxy.get("proxy_address", self.host)
            port = proxy.get("port", self.port)
            username = proxy.get("username", self.username)
            password = proxy.get("password", self.password)
            if username and password:
                url = f"http://{username}:{password}@{host}:{port}"
            else:
                url = f"http://{host}:{port}"
            return ProxyConfig(url=url, username=username, password=password)

        return ProxyConfig(
            url=self.rotating_proxy_url,
            username=self.username,
            password=self.password,
        )

    def get_random_proxy(self) -> ProxyConfig | None:
        """
        Get a random proxy from the list.
        """
        if not self.enabled:
            return None
        if self._proxy_list:
            proxy = random.choice(self._proxy_list)
            host = proxy.get("proxy_address", self.host)
            port = proxy.get("port", self.port)
            username = proxy.get("username", self.username)
            password = proxy.get("password", self.password)
            if username and password:
                url = f"http://{username}:{password}@{host}:{port}"
            else:
                url = f"http://{host}:{port}"
            return ProxyConfig(url=url, username=username, password=password)
        return ProxyConfig(
            url=self.rotating_proxy_url,
            username=self.username,
            password=self.password,
        )

    async def refresh_proxy_list(self) -> dict:
        """
        Refresh the proxy list via the Webshare API (POST).
        """
        if not self.api_key:
            raise ValueError("Webshare API key is required to refresh proxy list")

        url = f"{self.WEBSHARE_API_BASE}/proxy/list/refresh/"
        headers = {"Authorization": f"Token {self.api_key}"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers)
            response.raise_for_status()
            return response.json()

    def get_crawlberg_proxy_config(self) -> dict | None:
        """
        Get proxy configuration in the format expected by Crawlberg's ProxyConfig.
        """
        if not self.enabled:
            return None
        config = {"url": self.rotating_proxy_url}
        if self.username:
            config["username"] = self.username
        if self.password:
            config["password"] = self.password
        return config


_proxy_manager: ProxyManager | None = None


def get_proxy_manager() -> ProxyManager:
    global _proxy_manager
    if _proxy_manager is None:
        _proxy_manager = ProxyManager()
    return _proxy_manager
