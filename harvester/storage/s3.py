import hashlib
import os
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

    Supports both MinIO (local development) and Cloudflare R2 (production).
    Uses boto3 with the S3-compatible API.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        region: str | None = None,
        secure: bool | None = None,
    ):
        if settings.use_r2:
            self.endpoint = endpoint or settings.r2_endpoint or (
                f"https://{settings.r2_account_id}.r2.cloudflarestorage.com"
            )
            self.access_key = access_key or settings.r2_access_key_id
            self.secret_key = secret_key or settings.r2_secret_access_key
            self.bucket = bucket or settings.r2_bucket
            self.region = region or "auto"
            self.secure = True
        else:
            self.endpoint = endpoint or settings.s3_endpoint
            self.access_key = access_key or settings.s3_access_key
            self.secret_key = secret_key or settings.s3_secret_key
            self.bucket = bucket or settings.s3_bucket
            self.region = region or settings.s3_region
            self.secure = secure if secure is not None else settings.s3_secure

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

    def _ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket)
            logger.info("Bucket exists", bucket=self.bucket)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket"):
                logger.info("Creating bucket", bucket=self.bucket)
                try:
                    if self.region and self.region != "us-east-1" and not settings.use_r2:
                        self._client.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={
                                "LocationConstraint": self.region
                            },
                        )
                    else:
                        self._client.create_bucket(Bucket=self.bucket)
                except ClientError as create_err:
                    logger.warning(
                        "Could not create bucket",
                        bucket=self.bucket,
                        error=str(create_err),
                    )
            else:
                logger.warning(
                    "Could not check bucket",
                    bucket=self.bucket,
                    error=str(e),
                )

    def upload_file(
        self,
        local_path: str | Path,
        key: str,
        content_type: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """
        Upload a local file to the bucket. Returns the S3 key.
        """
        local_path = Path(local_path)
        extra_args: dict = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if metadata:
            extra_args["Metadata"] = {k: str(v) for k, v in metadata.items()}

        logger.info("Uploading file", local=str(local_path), key=key, bucket=self.bucket)
        self._client.upload_file(
            str(local_path),
            self.bucket,
            key,
            ExtraArgs=extra_args or None,
        )
        return key

    def upload_bytes(
        self,
        content: bytes,
        key: str,
        content_type: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """
        Upload raw bytes to the bucket. Returns the S3 key.
        """
        extra_args: dict = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if metadata:
            extra_args["Metadata"] = {k: str(v) for k, v in metadata.items()}

        logger.info("Uploading bytes", key=key, size=len(content), bucket=self.bucket)
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            **extra_args,
        )
        return key

    def upload_text(
        self,
        text: str,
        key: str,
        content_type: str = "text/plain",
    ) -> str:
        """
        Upload a text string to the bucket.
        """
        return self.upload_bytes(
            text.encode("utf-8"), key, content_type=content_type
        )

    def download_file(self, key: str, local_path: str | Path) -> Path:
        """
        Download a file from the bucket to a local path.
        """
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading file", key=key, local=str(local_path))
        self._client.download_file(self.bucket, key, str(local_path))
        return local_path

    def download_bytes(self, key: str) -> bytes:
        """
        Download an object as bytes.
        """
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def download_text(self, key: str) -> str:
        """
        Download an object as text.
        """
        return self.download_bytes(key).decode("utf-8")

    def delete_object(self, key: str) -> None:
        """
        Delete an object from the bucket.
        """
        logger.info("Deleting object", key=key, bucket=self.bucket)
        self._client.delete_object(Bucket=self.bucket, Key=key)

    def list_objects(self, prefix: str = "", max_keys: int = 1000) -> list[dict]:
        """
        List objects in the bucket with an optional prefix.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}
        ):
            for obj in page.get("Contents", []):
                objects.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                })
        return objects

    def object_exists(self, key: str) -> bool:
        """
        Check if an object exists in the bucket.
        """
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def get_object_metadata(self, key: str) -> dict:
        """
        Get metadata for an object.
        """
        response = self._client.head_object(Bucket=self.bucket, Key=key)
        return {
            "content_type": response.get("ContentType"),
            "content_length": response.get("ContentLength"),
            "etag": response.get("ETag", "").strip('"'),
            "last_modified": response.get("LastModified"),
            "metadata": response.get("Metadata", {}),
        }

    def generate_presigned_url(
        self, key: str, expires_in: int = 3600, method: str = "get_object"
    ) -> str:
        """
        Generate a presigned URL for an object.
        """
        return self._client.generate_presigned_url(
            method,
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def get_public_url(self, key: str) -> str:
        """
        Get the public URL for an object (if bucket is public).
        """
        return f"{self.endpoint.rstrip('/')}/{self.bucket}/{key}"

    def copy_object(self, source_key: str, dest_key: str) -> str:
        """
        Copy an object within the bucket.
        """
        copy_source = {"Bucket": self.bucket, "Key": source_key}
        self._client.copy_object(
            Bucket=self.bucket,
            Key=dest_key,
            CopySource=copy_source,
        )
        return dest_key

    @staticmethod
    def compute_hash(content: bytes) -> str:
        """
        Compute SHA-256 hash of content.
        """
        return hashlib.sha256(content).hexdigest()

    def build_key(self, job_run_id: int, doc_id: str, category: str, filename: str) -> str:
        """
        Build a standardized S3 key for a document artifact.

        Format: {job_run_id}/{doc_id}/{category}/{filename}
        Categories: original, parquet, json, markdown, embeddings, html
        """
        safe_filename = os.path.basename(filename)
        return f"job-{job_run_id}/{doc_id}/{category}/{safe_filename}"


_storage_client: StorageClient | None = None


def get_storage_client() -> StorageClient:
    global _storage_client
    if _storage_client is None:
        _storage_client = StorageClient()
    return _storage_client
