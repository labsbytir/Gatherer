import hashlib
from pathlib import Path
from typing import Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from harvester.config import get_settings
from harvester.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class StorageClient:
    """
    S3-compatible object storage client.
    Supports Cloudflare R2 (production) and MinIO (development).
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        region: str | None = None,
    ):
        if settings.use_r2:
            self.endpoint = endpoint or (
                f"https://{settings.r2_account_id}.r2.cloudflarestorage.com"
            )
            self.access_key = access_key or settings.r2_access_key_id
            self.secret_key = secret_key or settings.r2_secret_access_key
            self.bucket = bucket or settings.r2_bucket
            self.region = region or "auto"
        else:
            self.endpoint = endpoint or settings.s3_endpoint
            self.access_key = access_key or settings.s3_access_key
            self.secret_key = secret_key or settings.s3_secret_key
            self.bucket = bucket or settings.s3_bucket
            self.region = region or settings.s3_region

        if not self.bucket:
            raise ValueError("Storage bucket is required")

        self._client = boto3.client(
            service_name="s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchBucket"):
                logger.info("Creating bucket", bucket=self.bucket)
                try:
                    self._client.create_bucket(Bucket=self.bucket)
                except ClientError as ce:
                    logger.warning("Could not create bucket", error=str(ce))

    def upload_file(self, local_path: str | Path, key: str, content_type: str | None = None) -> str:
        extra = {"ContentType": content_type} if content_type else {}
        self._client.upload_file(str(local_path), self.bucket, key, ExtraArgs=extra or None)
        logger.info("Uploaded file", key=key, bucket=self.bucket)
        return key

    def upload_text(self, text: str, key: str, content_type: str = "text/plain") -> str:
        self._client.put_object(
            Bucket=self.bucket, Key=key, Body=text.encode("utf-8"),
            ContentType=content_type,
        )
        return key

    def upload_bytes(self, content: bytes, key: str, content_type: str | None = None) -> str:
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=self.bucket, Key=key, Body=content, **extra)
        return key

    def download_bytes(self, key: str) -> bytes:
        resp = self._client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    def download_text(self, key: str) -> str:
        return self.download_bytes(key).decode("utf-8")

    def delete(self, key: str):
        self._client.delete_object(Bucket=self.bucket, Key=key)

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> list[dict]:
        paginator = self._client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=prefix,
            PaginationConfig={"MaxItems": max_keys},
        ):
            for obj in page.get("Contents", []):
                objects.append({"key": obj["Key"], "size": obj["Size"]})
        return objects

    def presigned_url(self, key: str, expires_in: int = 3600) -> str:
        return self._client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=expires_in
        )

    @staticmethod
    def sha256(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()


_storage: StorageClient | None = None


def get_storage() -> StorageClient:
    global _storage
    if _storage is None:
        _storage = StorageClient()
    return _storage
