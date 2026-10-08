import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
import requests

from lakehouse_storage import config as LakehouseConfig

_LOCK = Lock()
_LAST_REQUEST = {}
_RETRYABLE = {408, 429, 500, 502, 503, 504}


def define_table_base_uri(isProductionEnv = False):
    if isProductionEnv:
        table_base_uri = f"s3://{LakehouseConfig.MINIO_BUCKET}/01_bronze_vault/"
    else:
        table_base_uri = f"../data/01_bronze_vault"
    
    return table_base_uri


def check_minio_connection(createIfNotExists = False):
    import boto3
    from botocore.config import Config

    endpoint = f"http://{LakehouseConfig.MINIO_ENDPOINT}"
    access_key = LakehouseConfig.MINIO_ACCESS_KEY
    secret_key = LakehouseConfig.MINIO_SECRET_KEY
    bucket = LakehouseConfig.MINIO_BUCKET
    
    print(f"Get lakehouse config done")

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )
        
        try:
            # Check if exists
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' exists!")
        except Exception:
            print(f"Bucket '{bucket}' not found, creating...")
            if createIfNotExists:
                print(f"Creating bucket '{bucket}'...")
                s3.create_bucket(Bucket=bucket)
                print(f"Bucket '{bucket}' created")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False



def datetime_validate_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if start > end:
        raise ValueError("[Error] start_date must be <= end_date")
    return start, end
    

def datetime_now_utc():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


# NOTE: Hien tai dang demo nen tam thoi chua xu ly phuc tap nhu nay
# TODO: Se su dung ham nay khi dua vao production thuc te
def get_response(url, source):
    """At most five GET attempts; one request/second/source in this process.

    Full jitter plus Retry-After for transient failures. Never disable TLS checks.
    A server delay over 120 seconds is surfaced instead of retrying too early.
    """
    for attempt in range(5):
        with _LOCK:
            delay = 1.0 - (time.monotonic() - _LAST_REQUEST.get(source, -1e9))
            if delay > 0:
                time.sleep(delay)
            _LAST_REQUEST[source] = time.monotonic()
        try:
            response = requests.get(url, timeout=(10, 60), headers={
                "User-Agent": "SmartDairyFactory-EducationalCrawler/1.0"
            })
        except requests.exceptions.SSLError:
            raise
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 4:
                raise
            time.sleep(random.uniform(0, min(30, 2 * 2 ** attempt)))
            continue
        if response.status_code not in _RETRYABLE or attempt == 4:
            response.raise_for_status()
            return response
        delay = random.uniform(0, min(30, 2 * 2 ** attempt))
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                seconds = float(retry_after)
            except ValueError:
                try:
                    stamp = parsedate_to_datetime(retry_after)
                    if stamp.tzinfo is None:
                        stamp = stamp.replace(tzinfo=timezone.utc)
                    seconds = (stamp - datetime.now(timezone.utc)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    seconds = 0
            delay = max(delay, seconds)
        if delay > 120:
            response.raise_for_status()
        response.close()
        time.sleep(delay)
    raise RuntimeError("HTTP retry loop exhausted")
