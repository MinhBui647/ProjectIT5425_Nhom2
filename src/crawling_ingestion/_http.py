"""Small shared HTTP helper for the USDA and FAO crawlers."""
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock

import requests

_LOCK = Lock()
_LAST_REQUEST = {}
_RETRYABLE = {408, 429, 500, 502, 503, 504}


def date_range(start_date, end_date):
    first = datetime.strptime(start_date, "%Y-%m-%d").date()
    last = datetime.strptime(end_date, "%Y-%m-%d").date()
    if first > last:
        raise ValueError("start_date must be <= end_date")
    return first, last


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
