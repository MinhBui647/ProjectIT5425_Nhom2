from datetime import datetime
import requests
import polars as pl

from crawling_ingestion.utils import datetime_validate_range, datetime_now_utc


USDA_BASE_API = "https://mpr.datamart.ams.usda.gov/services/v1.1/reports"


def _get_usda_reports():
    res = requests.get(USDA_BASE_API, timeout=15)
    if res.status_code == 200:
        data = res.json()
        return data
    print("[Error] Failed to get reports")
    return None


def _get_usda_report_section_detail(slug_id: str, section_name: str):
    res = requests.get(f"{USDA_BASE_API}/{slug_id}/{section_name}", timeout=15)
    if res.status_code == 200:
        data = res.json()
        return data
    print("[Error] Failed to get report detail")
    return None


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
            
                if dt_start_date <= result_date <= dt_end_date:
                    ranged_results.append(result)
            
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
    
            
            
        
    
    
