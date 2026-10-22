from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Harvester"
    app_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "INFO"

    database_url: str = "postgresql://harvester:harvester@localhost:5432/harvester"

    redis_url: str = "redis://localhost:6379/0"

    crawlberg_api_url: str = "http://localhost:3000"
    crawlberg_api_key: str | None = None

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "harvester-documents"
    s3_region: str = "us-east-1"
    s3_secure: bool = False

    r2_account_id: str | None = None
    r2_access_key_id: str | None = None
    r2_secret_access_key: str | None = None
    r2_bucket: str | None = None
    r2_endpoint: str | None = None

    webshare_api_key: str | None = None
    webshare_proxy_username: str | None = None
    webshare_proxy_password: str | None = None
    webshare_proxy_host: str = "p.webshare.io"
    webshare_proxy_port: int = 80
    webshare_use_proxy: bool = False

    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_device: str = "cpu"
    embedding_batch_size: int = 32
    embedding_chunk_size: int = 512
    embedding_chunk_overlap: int = 50

    ocr_languages: str = "eng"
    ocr_dpi: int = 300

    crawler_user_agent: str = "Harvester/1.0 (+https://github.com/labsbytir/Gatherer)"
    crawler_max_depth: int = 3
    crawler_max_pages: int = 1000
    crawler_concurrent_requests: int = 4
    crawler_download_timeout: int = 60
    crawler_max_file_size: int = 104857600
    crawler_respect_robots: bool = True

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_reload: bool = False

    celery_concurrency: int = 2
    celery_log_level: str = "INFO"

    temp_dir: str = "/tmp/harvester"
    job_poll_interval: int = 5
    job_poll_max_attempts: int = 720

    @property
    def use_r2(self) -> bool:
        return bool(self.r2_account_id and self.r2_access_key_id and self.r2_secret_access_key)

    @property
    def celery_broker_url(self) -> str:
        return self.redis_url

    @property
    def celery_result_backend(self) -> str:
        return self.redis_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
