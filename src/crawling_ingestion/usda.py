"""USDA AMS report 2993: weekly dairy sales prices, in USD/lb."""
import json
import math
from datetime import datetime
from urllib.parse import quote

import polars as pl

from crawling_ingestion._http import date_range, get_response, utc_now

USDA_BASE_API = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports/2993"
USDA_PRODUCT_SECTIONS = {
    "BUTTER": "Final Butter Prices and Sales",
    "CHEDDAR_40LB": "Final 40 Pound Block Cheddar Cheese Prices and Sales",
    "CHEDDAR_500LB": "Final 500 Pound Barrel Cheddar Cheese Prices, Sales, and Moisture Content",
    "DRY_WHEY": "Final Dry Whey Prices and Sales",
    "NONFAT_DRY_MILK": "Final Nonfat Dry Milk Prices and Sales",
}
USDA_SCHEMA = {
    "source": pl.String, "product": pl.String, "contract": pl.String,
    "observed_at": pl.String, "price": pl.Float64, "currency": pl.String,
    "unit": pl.String, "ingested_at": pl.String,
    "_source_system": pl.String, "_generated_at": pl.String,
    "_created_at": pl.String, "_source_url": pl.String,
    "_raw_record": pl.String, "_price_status": pl.String,
}


def get_usda_marketprice_data(start_date: str, end_date: str,
                              product_codes: list[str] | None = None) -> pl.DataFrame:
    """Fetch inclusive YYYY-MM-DD range by week_ending_date.

    None selects all five products. Return one DataFrame (also when empty).
    Missing/suppressed/invalid prices remain null with a status and raw JSON;
    they are never changed to zero or silently discarded from Bronze.
    HTTP failures, incomplete responses and duplicate weeks raise exceptions.
    """
    first, last = date_range(start_date, end_date)
    products = list(USDA_PRODUCT_SECTIONS) if product_codes is None else list(product_codes)
    if not products or len(set(products)) != len(products):
        raise ValueError("product_codes must be nonempty and unique")
    unknown = set(products) - set(USDA_PRODUCT_SECTIONS)
    if unknown:
        raise ValueError(f"Unknown USDA product codes: {sorted(unknown)}")
    records = []
    seen = set()
    for product in products:
        url = f"{USDA_BASE_API}/{quote(USDA_PRODUCT_SECTIONS[product], safe='')}"
        response = get_response(url, "usda")
        try:
            payload = response.json()
        finally:
            response.close()
        fetched = utc_now()
        rows = payload.get("results")
        total = payload.get("stats", {}).get("totalRows:")
        if not isinstance(rows, list) or total is None or int(total) != len(rows):
            raise ValueError("USDA response is incomplete or its schema changed")
        if payload.get("reportSection") != USDA_PRODUCT_SECTIONS[product]:
            raise ValueError("USDA returned an unexpected report section")
        for row in rows:
            day = datetime.strptime(row["week_ending_date"], "%m/%d/%Y").date()
            if not first <= day <= last:
                continue
            key = (product, day)
            if key in seen:
                raise ValueError(f"Duplicate USDA product/week: {key}")
            seen.add(key)
            fields = [key for key in row if key.lower().endswith("_price") and "wtd" not in key.lower()]
            if len(fields) != 1:
                raise ValueError("USDA price field missing or ambiguous")
            raw_price = row[fields[0]]
            try:
                price = float(str(raw_price).replace(",", "").strip())
                valid = math.isfinite(price) and price > 0 and not isinstance(raw_price, bool)
            except (ValueError, TypeError):
                price, valid = None, False
            records.append({
                "source": "USDA_NDPSR", "product": product, "contract": "WEEKLY_SURVEY",
                "observed_at": day.isoformat() + "T00:00:00Z",
                "price": price if valid else None, "currency": "USD", "unit": "lb",
                "ingested_at": fetched, "_source_system": "usda_ndpsr",
                "_generated_at": fetched, "_created_at": fetched[:10],
                "_source_url": url, "_raw_record": json.dumps(row, ensure_ascii=False),
                "_price_status": "OK" if valid else "MISSING_OR_INVALID",
            })
    return pl.DataFrame(records, schema=USDA_SCHEMA).sort(["observed_at", "product"])
