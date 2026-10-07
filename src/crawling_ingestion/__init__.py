from crawling_ingestion.openmeteo import get_openmeteo_weather_data
from crawling_ingestion.openfda import get_openfda_foodrecall_data
from crawling_ingestion.gdt import get_gdt_events, get_gdt_marketprice_data
from crawling_ingestion.usda import get_usda_marketprice_data
from crawling_ingestion.fao import get_fao_dairyindex_data


def run_crawlers(*args, **kwargs):
    # Lazy import lets python -m crawling_ingestion.run_crawlers run without a cycle.
    from crawling_ingestion.run_crawlers import run_crawlers as run
    return run(*args, **kwargs)


__all__ = ["run_crawlers", "get_openmeteo_weather_data", "get_openfda_foodrecall_data",
           "get_gdt_events", "get_gdt_marketprice_data", "get_usda_marketprice_data",
           "get_fao_dairyindex_data"]
