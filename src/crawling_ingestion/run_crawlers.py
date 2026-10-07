"""Run selected crawlers and write daily Parquet + SHA-256 batches locally."""
import argparse
from pathlib import Path

import polars as pl
from lakehouse_storage import FARM_LOCATIONS, write_bronze_batch
from crawling_ingestion._http import date_range
from crawling_ingestion.openmeteo import get_openmeteo_weather_data
from crawling_ingestion.openfda import get_openfda_foodrecall_data
from crawling_ingestion.gdt import get_gdt_marketprice_data
from crawling_ingestion.usda import get_usda_marketprice_data
from crawling_ingestion.fao import get_fao_dairyindex_data

TABLE_BASE_URI = Path(__file__).resolve().parents[2] / "data" / "01_bronze_vault"
SOURCES = ("openmeteo", "openfda", "gdt", "usda", "fao")


def _write_daily(df, table_name, date_column, bronze_root):
    if df is None:
        raise RuntimeError(f"Crawler returned no response for {table_name}")
    if df.is_empty():
        print(f"{table_name}: no records in the selected range")
        return []
    # GDT/FDA source schemas have different date-column names.
    days = df[date_column].str.slice(0, 10).unique().sort().to_list()
    paths = []
    for day in days:
        batch = df.filter(pl.col(date_column).str.slice(0, 10) == day)
        paths.append(write_bronze_batch(batch, table_name, bronze_root=bronze_root))
    print(f"{table_name}: {len(df)} rows -> {len(paths)} Bronze files")
    return paths


def run_crawlers(start_date="2025-01-01", end_date="2025-02-01",
                 sources=None, bronze_root=None):
    first, last = date_range(start_date, end_date)
    selected = list(SOURCES) if sources is None else list(sources)
    if not selected or len(set(selected)) != len(selected) or set(selected) - set(SOURCES):
        raise ValueError(f"sources must contain unique values from {SOURCES}")
    root = TABLE_BASE_URI if bronze_root is None else bronze_root
    if str(root).startswith("s3://"):
        raise ValueError("The current Bronze writer supports local paths only")
    paths = []
    for source in selected:
        if source == "openmeteo":
            for farm_id, location in FARM_LOCATIONS.items():
                df = get_openmeteo_weather_data(farm_id, location["lat"], location["lon"], start_date, end_date)
                paths += _write_daily(df, "weather", "observed_at", root)
        elif source == "openfda":
            # Existing crawler uses expected_total to decide how many pages to fetch.
            from crawling_ingestion._http import get_response
            from urllib.parse import urlencode
            query = f"report_date:[{first:%Y%m%d} TO {last:%Y%m%d}]"
            response = get_response("https://api.fda.gov/food/enforcement.json?" + urlencode({"search": query, "limit": 1}), "openfda")
            try:
                total = int(response.json()["meta"]["results"]["total"])
            finally:
                response.close()
            if total > 26000:
                raise ValueError("openFDA range exceeds skip limit; select a shorter date range")
            dfs = get_openfda_foodrecall_data(f"{first:%Y%m%d}", f"{last:%Y%m%d}", total)
            if sum(len(df) for df in dfs) != total:
                raise RuntimeError("openFDA returned incomplete results")
            for df in dfs:
                paths += _write_daily(df, "food_recalls", "report_date", root)
        elif source == "gdt":
            df = get_gdt_marketprice_data(start_date, end_date, ["AMF", "SMP", "WMP"])
            paths += _write_daily(df, "market_prices", "EventDate", root)
        elif source == "usda":
            df = get_usda_marketprice_data(start_date, end_date)
            paths += _write_daily(df, "usda_market_prices", "observed_at", root)
        elif source == "fao":
            df = get_fao_dairyindex_data(start_date, end_date)
            paths += _write_daily(df, "market_indices", "period_start", root)
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--end-date", default="2025-02-01")
    parser.add_argument("--sources", nargs="+", choices=SOURCES, default=list(SOURCES))
    parser.add_argument("--bronze-root", type=Path, default=TABLE_BASE_URI)
    args = parser.parse_args()
    run_crawlers(args.start_date, args.end_date, args.sources, args.bronze_root)


if __name__ == "__main__":
    main()
