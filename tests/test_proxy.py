import pytest

from harvester.proxy import ProxyManager


class TestProxyManager:
    def test_disabled_proxy(self):
        manager = ProxyManager(enabled=False)
        assert manager.get_proxy_config() is None
        assert manager.get_httpx_proxies() is None
        assert manager.get_crawlberg_proxy_config() is None

    def test_rotating_proxy_url(self):
        manager = ProxyManager(
            enabled=True,
            username="user123",
            password="pass456",
            host="proxy.example.com",
            port=8080,
        )
        assert manager.rotating_proxy_url == "http://user123:pass456@proxy.example.com:8080"

    def test_rotating_proxy_url_no_auth(self):
        manager = ProxyManager(
            enabled=True,
            host="proxy.example.com",
            port=3128,
        )
        assert manager.rotating_proxy_url == "http://proxy.example.com:3128"

    def test_get_proxy_config(self):
        manager = ProxyManager(
            enabled=True,
            username="user",
            password="pass",
            host="p.webshare.io",
            port=80,
        )
        config = manager.get_proxy_config()
        assert config is not None
        assert "p.webshare.io" in config.url
        assert config.username == "user"

    def test_get_httpx_proxies(self):
        manager = ProxyManager(
            enabled=True,
            username="user",
            password="pass",
            host="p.webshare.io",
            port=80,
        )
        proxies = manager.get_httpx_proxies()
        assert proxies is not None
        assert "http://" in proxies
        assert "https://" in proxies

    def test_crawlberg_proxy_config(self):
        manager = ProxyManager(
            enabled=True,
            username="user",
            password="pass",
            host="p.webshare.io",
            port=80,
        )
        config = manager.get_crawlberg_proxy_config()
        assert config is not None
        assert "url" in config
        assert config["username"] == "user"
        assert config["password"] == "pass"
