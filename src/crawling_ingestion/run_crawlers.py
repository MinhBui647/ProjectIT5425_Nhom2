from datetime import datetime

from lakehouse_storage import (
    FARM_LOCATIONS, GDT_PRODUCT_CODES, USDA_REPORT_KEYWORD,
    write_bronze_batch,
)
from crawling_ingestion.utils import define_table_base_uri, check_minio_connection
from crawling_ingestion.openmeteo import get_openmeteo_weather_data
from crawling_ingestion.openfda import get_openfda_foodrecall_data
from crawling_ingestion.gdt import get_gdt_marketprice_data
from crawling_ingestion.fao import get_fao_marketindices_data
from crawling_ingestion.usda import get_usda_marketreports_data


# TODO: Chuyen sang True de chay flow cho production
PRODUCTION_ENV = False


# CONFIG
TABLE_BASE_URI = define_table_base_uri(PRODUCTION_ENV)

# TODO: Hardcode start - end date to test
START_DATE = datetime(2025, 1, 1).strftime("%Y-%m-%d")
END_DATE = datetime(2025, 2, 1).strftime("%Y-%m-%d")



def run_crawlers():
    # NOTE: Map from source name to schema name (lakehouse_storage/schema.py)
    TABLE_NAMES = {
        "openmeteo": "weather",
        "openfda": "food_recalls",
        "gdt": "market_prices",
        "fao": "market_indices",
        "usda": "market_reports",
    }
    
    # # 1. OpenMeteo
    # for farm_id, location in FARM_LOCATIONS.items():
    #     df = get_openmeteo_weather_data(
    #         farm_id=farm_id,
    #         latitude=location["lat"],
    #         longitude=location["lon"],
    #         start_date=START_DATE,
    #         end_date=END_DATE,
    #     )
    #     write_bronze_batch(
    #         df=df, 
    #         table_name=TABLE_NAMES["openmeteo"],
    #         bronze_root=TABLE_BASE_URI,
    #     )
    
    
    # 2. OpenFDA
    openfda_dfs = get_openfda_foodrecall_data(
        start_date=START_DATE,
        end_date=END_DATE,
        expected_total=7
    )
    
    for df in openfda_dfs:
        write_bronze_batch(
            df=df,
            table_name=TABLE_NAMES["openfda"],
            bronze_root=TABLE_BASE_URI,
        )
    
    
    # # 3. GDT
    # gdt_df = get_gdt_marketprice_data(
    #     start_date=START_DATE,
    #     end_date=END_DATE,
    #     product_codes=GDT_PRODUCT_CODES
    # )
    
    # write_bronze_batch(
    #     df=gdt_df,
    #     table_name=TABLE_NAMES["gdt"],
    #     bronze_root=TABLE_BASE_URI,
    # )
    
    
    # # 4. FAO
    # fao_df = get_fao_marketindices_data(
    #     start_date=START_DATE,
    #     end_date=END_DATE,
    # )
    
    # write_bronze_batch(
    #     df=fao_df,
    #     table_name=TABLE_NAMES["fao"],
    #     bronze_root=TABLE_BASE_URI,
    # )
    
    
    # # 5. USDA
    # usda_dfs = get_usda_marketreports_data(
    #     start_date=START_DATE,
    #     end_date=END_DATE,
    #     keyword=USDA_REPORT_KEYWORD
    # )
    
    # for df in usda_dfs:
    #     # NOTE: Do API response k dong nhat (moi df mot schema khac nhau) -> k check schema -> xu ly sau o silver
    #     # TODO: Schema trong lakehouse_storage/schemas.py cua table nay dang de tam placeholder
    #     write_bronze_batch(
    #         df=df,
    #         table_name=TABLE_NAMES["usda"],
    #         bronze_root=TABLE_BASE_URI,
    #         validate_schema=False
    #     )



if __name__ == "__main__":
    # Check connection
    if PRODUCTION_ENV:
        isMinioConnected = check_minio_connection()
    else:
        isMinioConnected = True

    # Run all the crawlers 
    if isMinioConnected:
        run_crawlers()
