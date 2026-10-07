"""FAO monthly Dairy Price Index; base period 2014-2016=100."""
import csv
import io
import json
import math
import re
from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import polars as pl

from crawling_ingestion._http import date_range, get_response, utc_now

FAO_PAGE_URL = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
FAO_BASE_PERIOD = "2014-2016=100"
FAO_SCHEMA = {
    "source": pl.String, "index_name": pl.String, "period_start": pl.String,
    "index_value": pl.Float64, "base_period": pl.String, "ingested_at": pl.String,
    "_source_system": pl.String, "_generated_at": pl.String,
    "_created_at": pl.String, "_source_url": pl.String,
    "_raw_record": pl.String, "_value_status": pl.String,
}


class _CsvLinks(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href", "")
        if tag == "a" and urlsplit(href).path.endswith("/food_price_indices_data.csv"):
            self.links.append(urljoin(FAO_PAGE_URL, href))


def get_fao_dairyindex_data(start_date: str, end_date: str) -> pl.DataFrame:
    """Fetch monthly DAIRY index, filtering inclusive YYYY-MM-DD month starts.

    E.g. Jan 15-Feb 28 selects February only. Values are index points, not USD.
    Preserve source cells as JSON; missing values stay null with a status.
    """
    first, last = date_range(start_date, end_date)
    page = get_response(FAO_PAGE_URL, "fao")
    try:
        parser = _CsvLinks()
        parser.feed(page.text)
    finally:
        page.close()
    if not parser.links:
        raise ValueError("FAO monthly CSV link not found; inspect Download datasets")
    url = parser.links[0]
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not (parsed.hostname == "fao.org" or (parsed.hostname or "").endswith(".fao.org")):
        raise ValueError("Unexpected FAO download host")
    response = get_response(url, "fao")
    try:
        text = response.content.decode("utf-8-sig")
    finally:
        response.close()
    return _parse_fao_csv(text, first, last, url, utc_now())


def _parse_fao_csv(text, first, last, url, fetched):
    if FAO_BASE_PERIOD not in re.sub(r"\s+", "", text):
        raise ValueError("FAO base period changed; update the adapter")
    rows = list(csv.reader(io.StringIO(text)))
    header_index = next((i for i, row in enumerate(rows)
                         if row and row[0].strip() == "Date" and "Dairy" in row), None)
    if header_index is None:
        raise ValueError("FAO CSV header changed")
    header = rows[header_index]
    dairy_index = header.index("Dairy")
    records, seen = [], set()
    for row in rows[header_index + 1:]:
        if not row or not row[0].strip():
            continue
        month = row[0].strip()
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            raise ValueError(f"Unexpected FAO monthly period: {month}")
        day = datetime.strptime(month + "-01", "%Y-%m-%d").date()
        if not first <= day <= last:
            continue
        if month in seen:
            raise ValueError(f"Duplicate FAO month: {month}")
        seen.add(month)
        if len(row) <= dairy_index:
            raise ValueError(f"Incomplete FAO row: {month}")
        try:
            value = float(row[dairy_index].replace(",", "").strip())
            valid = math.isfinite(value) and value > 0
        except ValueError:
            value, valid = None, False
        records.append({
            "source": "FAO", "index_name": "DAIRY", "period_start": day.isoformat(),
            "index_value": value if valid else None, "base_period": FAO_BASE_PERIOD,
            "ingested_at": fetched, "_source_system": "fao_food_price_index",
            "_generated_at": fetched, "_created_at": fetched[:10], "_source_url": url,
            "_raw_record": json.dumps({"header": header, "cells": row}, ensure_ascii=False),
            "_value_status": "OK" if valid else "MISSING_OR_INVALID",
        })
    return pl.DataFrame(records, schema=FAO_SCHEMA).sort("period_start")
