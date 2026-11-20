"""Lazy live wrappers reuse P2 collectors without importing its executable runner."""
from datetime import timedelta
import importlib
import polars as pl

COLLECTORS = {
    'market_prices': ('gdt', 'get_gdt_marketprice_data'),
    'market_indices': ('fao', 'get_fao_marketindices_data'),
    'weather': ('openmeteo', 'get_openmeteo_weather_data'),
    'food_recalls': ('openfda', 'get_openfda_foodrecall_data'),
    'market_reports': ('usda', 'get_usda_marketreports_data'),
}
DATE_COLUMNS = {'market_prices': 'EventDate', 'weather': 'observed_at', 'food_recalls': 'report_date', 'market_reports': 'published_date'}


def collect_live(config):
    """Yield raw batches, preserving NO_DATA; upstream dates are inclusive."""
    start, end = config.utc_window()
    window = {'start_date': start.date().isoformat(), 'end_date': (end - timedelta(microseconds=1)).date().isoformat()}
    for table in config.sources:
        module, function = COLLECTORS[table]
        collect = getattr(importlib.import_module(f'crawling_ingestion.{module}'), function)
        if table == 'weather':
            if not config.farms:
                raise ValueError('live weather requires farm coordinates')
            results = [collect(farm_id=farm, latitude=lat, longitude=lon, **window) for farm, lat, lon in config.farms]
        else:
            result = collect(**window, **({'expected_total': config.expected_recalls} if table == 'food_recalls' else {}))
            results = result if isinstance(result, list) else [result]
        if not results:
            results = [None]
        for frame in results:
            if frame is None or frame.is_empty():
                yield table, []
                continue
            # The crawler accepts dates; enforce sub-day boundaries at collection.
            column = DATE_COLUMNS.get(table)
            if column in frame.columns:
                fmt = '%Y%m%d' if table == 'food_recalls' else ('%m/%d/%Y %H:%M:%S' if table == 'market_reports' else None)
                parsed = pl.col(column).cast(pl.String).str.to_datetime(format=fmt, time_zone='UTC', strict=False)
                frame = frame.filter((parsed >= start) & (parsed < end))
            yield table, frame.to_dicts()
