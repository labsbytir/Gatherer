import pytest

from harvester.proxy import WebshareProxy


class TestWebshareProxy:
    def test_disabled(self):
        proxy = WebshareProxy(enabled=False)
        assert proxy.get_proxy_config() is None
        assert proxy.get_crawlberg_proxy() is None
        assert proxy.get_env_proxies() is None

    def test_rotating_endpoint_with_auth(self):
        proxy = WebshareProxy(
            enabled=True, username="user", password="pass",
            host="p.webshare.io", port=80,
        )
        assert proxy.rotating_endpoint == "http://user:pass@p.webshare.io:80"

    def test_rotating_endpoint_no_auth(self):
        proxy = WebshareProxy(enabled=True, host="proxy.example.com", port=3128)
        assert proxy.rotating_endpoint == "http://proxy.example.com:3128"

    def test_crawlberg_proxy_config(self):
        proxy = WebshareProxy(
            enabled=True, username="user", password="pass",
            host="p.webshare.io", port=80,
        )
        cfg = proxy.get_crawlberg_proxy()
        assert cfg is not None
        assert cfg["url"] == "http://user:pass@p.webshare.io:80"
        assert cfg["username"] == "user"
        assert cfg["password"] == "pass"

    def test_env_proxies(self):
        proxy = WebshareProxy(
            enabled=True, username="u", password="p",
            host="h", port=80,
        )
        env = proxy.get_env_proxies()
        assert env is not None
        assert "HTTP_PROXY" in env
        assert "HTTPS_PROXY" in env
