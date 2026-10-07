from datetime import datetime

from lakehouse_storage import (
    config as LakehouseConfig,
    FARM_LOCATIONS,
    write_bronze_batch,
)
from crawling_ingestion.openmeteo import get_openmeteo_weather_data
from crawling_ingestion.openfda import get_openfda_foodrecall_data
from crawling_ingestion.gdt import get_gdt_marketprice_data



# Cai nay se chuyen sang folder utils chung sau nay
def check_minio_connection():
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
            # If not -> create
            print(f"Bucket '{bucket}' not found, creating...")
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket '{bucket}'")

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False



# CONFIG

# TODO: Se thay local path bang minio path sau khi test crawl cac source xong
# Local folder path
TABLE_BASE_URI = f"../data/01_bronze_vault"
# MinIO path
# TABLE_BASE_URI = f"s3://{LakehouseConfig.MINIO_BUCKET}/01_bronze_vault/"

START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 2, 1)


def run_crawlers():
    
    # TODO: Dang hardcode cac table_name -> co the update sau
    
    # 1. OpenMeteo
    openmeteo_table_name = "weather"
    
    for farm_id, location in FARM_LOCATIONS.items():
        df = get_openmeteo_weather_data(
            farm_id=farm_id,
            latitude=location["lat"],
            longitude=location["lon"],
            start_date=START_DATE.strftime("%Y-%m-%d"),
            end_date=END_DATE.strftime("%Y-%m-%d"),
        )
        write_bronze_batch(
            df=df, 
            table_name=openmeteo_table_name,
            bronze_root=TABLE_BASE_URI,
        )
    
    
    # 2. OpenFDA
    openfda_table_name = "food_recalls"
    
    openfda_dfs = get_openfda_foodrecall_data(
        start_date=START_DATE.strftime("%Y%m%d"),
        end_date=END_DATE.strftime("%Y%m%d"),
        expected_total=7
    )
    
    for df in openfda_dfs:
        write_bronze_batch(
            df=df,
            table_name=openfda_table_name,
            bronze_root=TABLE_BASE_URI,
        )
    
    
    # 3. GDT
    # TODO: Dinh nghia lai cac `product_codes` se crawl o trong lakehouse
    gdt_table_name = "market_prices"
    
    gdt_df = get_gdt_marketprice_data(
        start_date=START_DATE.strftime("%Y-%m-%d"),
        end_date=END_DATE.strftime("%Y-%m-%d"),
        product_codes=["AMF", "SMP", "WMP"]
    )
    
    write_bronze_batch(
        df=gdt_df,
        table_name=gdt_table_name,
        bronze_root=TABLE_BASE_URI,
    )
    


if __name__ == "__main__":
    # Check connection
    isMinioConnected = check_minio_connection()

    # Run all the crawlers 
    if isMinioConnected:
        run_crawlers()
