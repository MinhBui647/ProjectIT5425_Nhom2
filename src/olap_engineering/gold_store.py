"""Gold layer on MinIO: s3://<bucket>/03_gold_marts/<table>.parquet (TV4).

build_marts.py --upload-minio pushes the local Gold Parquet files here;
chdb_analytics_engine.py --from-minio reads them back with ClickHouse s3().
"""

import os
from pathlib import Path

GOLD_S3_PREFIX = "03_gold_marts"


def _bucket() -> str:
    return os.getenv("MINIO_BUCKET", "smart-dairy-lakehouse")


def gold_s3_key(table: str) -> str:
    return f"{GOLD_S3_PREFIX}/{table}.parquet"


def gold_s3_uri(table: str) -> str:
    return f"s3://{_bucket()}/{gold_s3_key(table)}"


def upload_gold_dir(local_dir: str | Path) -> dict[str, int]:
    """Upload every Gold Parquet file in local_dir; return {object key: bytes}."""
    import boto3
    from botocore.config import Config

    from lakehouse_storage.config import (
        MINIO_ACCESS_KEY,
        MINIO_ENDPOINT,
        MINIO_SECRET_KEY,
        MINIO_SECURE,
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    bucket = _bucket()
    s3.head_bucket(Bucket=bucket)  # fail fast if MinIO is down or the bucket is missing

    uploaded = {}
    for path in sorted(Path(local_dir).glob("*.parquet")):
        key = gold_s3_key(path.stem)
        s3.upload_file(str(path), bucket, key)
        uploaded[key] = path.stat().st_size
    return uploaded


def chdb_s3_source(table: str) -> str:
    """ClickHouse s3() table function reading one Gold file from MinIO.

    Runs inside the dairy-olap container, where MinIO is reached through
    host.docker.internal (override with GOLD_S3_ENDPOINT).
    """
    endpoint = os.getenv("GOLD_S3_ENDPOINT", "http://host.docker.internal:9000").rstrip("/")
    key = os.getenv("MINIO_ACCESS_KEY", "dairy")
    secret = os.getenv("MINIO_SECRET_KEY", "hustit5425")
    url = f"{endpoint}/{_bucket()}/{gold_s3_key(table)}"
    return f"s3('{url}', '{key}', '{secret}', 'Parquet')"
