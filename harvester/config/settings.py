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
    crawlberg_poll_interval: int = 5
    crawlberg_poll_max_attempts: int = 720

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

    webshare_api_key: str | None = None
    webshare_proxy_username: str | None = None
    webshare_proxy_password: str | None = None
    webshare_proxy_host: str = "p.webshare.io"
    webshare_proxy_port: int = 80
    webshare_use_proxy: bool = False

    ocr_languages: str = "eng"
    parquet_converter_path: str = "/usr/local/bin/parquet-converter"
    use_parquet_converter: bool = True

    default_job_run_id: int = 10
    max_depth: int = 3
    max_pages: int = 1000
    max_file_size: int = 104857600

    temp_dir: str = "/tmp/harvester"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def use_r2(self) -> bool:
        return bool(self.r2_account_id and self.r2_access_key_id and self.r2_secret_access_key)


@lru_cache
def get_settings() -> Settings:
    return Settings()
