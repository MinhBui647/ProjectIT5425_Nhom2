from datetime import datetime
import ipaddress
import json
import logging
import socket
import subprocess
from urllib.parse import quote, urlsplit
import requests
import polars as pl

from crawling_ingestion.utils import datetime_validate_range, datetime_now_utc


USDA_BASE_API = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports"
_LOGGER = logging.getLogger(__name__)


def _is_dns_error(error):
    """Inspect wrapped requests/urllib3 errors without matching error messages."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, socket.gaierror):
            return True
        pending.extend(item for item in (
            current.__cause__, current.__context__, getattr(current, "reason", None),
            *current.args,
        ) if isinstance(item, BaseException))
    return False


def _get_json_with_public_dns(url):
    """Windows DNS fallback, scoped to this request; TLS remains verified."""
    host = urlsplit(USDA_BASE_API).hostname
    if urlsplit(url).hostname != host or urlsplit(url).scheme != "https":
        raise ValueError("DNS fallback is restricted to the USDA HTTPS API")
    lookup = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
         "$ErrorActionPreference = 'Stop'; "
         "Resolve-DnsName 'mpr.datamart.ams.usda.gov' -Type A -Server 8.8.8.8 -DnsOnly | "
         "Where-Object { $_.Type -eq 'A' } | "
         "Select-Object -First 1 -ExpandProperty IPAddress"],
        capture_output=True, text=True, timeout=30, check=True,
    )
    address = str(ipaddress.IPv4Address(lookup.stdout.strip()))
    response = subprocess.run(
        ["curl.exe", "--fail", "--silent", "--show-error",
         "--connect-timeout", "20", "--max-time", "60",
         "--resolve", f"{host}:443:{address}", url],
        capture_output=True, encoding="utf-8", timeout=70, check=True,
    )
    return json.loads(response.stdout)


def _get_usda_json(url):
    try:
        response = requests.get(url, timeout=(15, 60))
    except requests.ConnectionError as error:
        if not _is_dns_error(error):
            raise
        _LOGGER.warning("USDA local DNS failed; retrying with public DNS (8.8.8.8).")
        try:
            return _get_json_with_public_dns(url)
        except (OSError, subprocess.SubprocessError, ValueError) as fallback_error:
            raise RuntimeError(
                "USDA DNS fallback failed. Check internet access, public DNS and curl.exe. "
                f"Details: {getattr(fallback_error, 'stderr', None) or fallback_error}"
            ) from fallback_error
    response.raise_for_status()
    return response.json()


def _get_usda_reports():
    return _get_usda_json(USDA_BASE_API)


def _get_usda_report_section_detail(slug_id: str, section_name: str):
    return _get_usda_json(
        f"{USDA_BASE_API}/{quote(str(slug_id), safe='')}/{quote(section_name, safe='')}"
    )


def _find_usda_report_by_keyword(keyword: str):
    all_reports = _get_usda_reports()
    if all_reports is None:
        return []
    
    slug_ids = []
    filtered_reports = []
    
    for rp in all_reports:
        # Filter bao cao co keyword can tim
        if keyword.lower() in str(rp["report_title"]).lower():
            # Tranh duplicate
            if rp["slug_id"] not in slug_ids:
                filtered_reports.append(rp)
                slug_ids.append(rp["slug_id"])
    
    return filtered_reports



def get_usda_marketreports_data(
    start_date: str,
    end_date: str,
    keyword: str = "dairy",
):
    """
        Params:
            start_date: str, format "YYYY-mm-dd"
            end_date: str, format "YYYY-mm-dd"
        
        Returns:
            Dict | None
    """
        
    dt_start_date, dt_end_date = datetime_validate_range(start_date, end_date)
    
    dfs = []
    
    filtered_reports = _find_usda_report_by_keyword(keyword)

    
    for rp in filtered_reports:
        report_sections = rp["sectionNames"]
        
        for section_name in report_sections:
            section = _get_usda_report_section_detail(rp["slug_id"], section_name)
            results = section["results"]
            
            ranged_results = []
            
            for result in results:
                result_date = datetime.strptime(result["published_date"], "%m/%d/%Y %H:%M:%S")
            
                if dt_start_date.date() <= result_date.date() <= dt_end_date.date():
                    ranged_results.append(result)

            if not ranged_results:
                continue
            
            df = pl.DataFrame(ranged_results, infer_schema_length=None)
            df = df.with_columns([
                pl.lit(rp["slug_id"]).alias("meta_slug_id"),
                pl.lit(section_name).alias("meta_section_name"),
                pl.lit(section["stats"]["totalRows:"]).alias("meta_total_rows"),
                pl.lit(section["stats"]["returnedRows:"]).alias("meta_returned_rows"),

                pl.lit(USDA_BASE_API).alias("_base_api"),
                pl.lit(datetime_now_utc()).alias("_created_at"),
            ])
            
            dfs.append(df)
    
    return dfs
    
            
            
        
    
    
