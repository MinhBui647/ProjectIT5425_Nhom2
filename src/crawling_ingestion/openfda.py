import math
import json
import polars as pl
import requests
from datetime import datetime

# API query note: limit <= 1000, skip <= 25000
OPENFDA_MAX_QUERY_LIMIT = 1000
OPENFDA_MAX_QUERY_SKIP = 25000


def get_openfda_foodrecall_data(
    start_date: str,
    end_date: str,
    expected_total: int,
):
    """
    Params:
        start_date: str, format "YYYYmmdd"
        end_date: str, format "YYYYmmdd"
        expected_total: int
    
    Returns:
        Dict | None
    """
    
    openfda_api = f"https://api.fda.gov/food/enforcement.json?search=report_date:[{start_date}+TO+{end_date}]"
    pages = math.ceil(expected_total / OPENFDA_MAX_QUERY_LIMIT) 
    dfs = []
    
    for page in range(pages):
        if page > OPENFDA_MAX_QUERY_SKIP:
            print("Reach the limitations of OpenFDA API")
            break
        
        pagination_query = f"&skip={page * OPENFDA_MAX_QUERY_LIMIT}&limit={OPENFDA_MAX_QUERY_LIMIT}"
        openfda_paginated_api = openfda_api + pagination_query
        
        res = requests.get(openfda_paginated_api, timeout=15)
        
        if res.status_code == 200:
            data = res.json()
            
            # Results
            df = pl.DataFrame(data["results"])
            
            df = df.with_columns([
                # Metadata of the results
                pl.lit(data["meta"]["last_updated"]).alias("meta_last_updated"),
                pl.lit(data["meta"]["results"]["skip"]).alias("meta_results_skip"),
                pl.lit(data["meta"]["results"]["limit"]).alias("meta_results_limit"),
                pl.lit(data["meta"]["results"]["total"]).alias("meta_results_total"),
                
                # Additional context
                pl.lit(expected_total).alias("_expected_total"),
                pl.lit(datetime.now().strftime("%Y-%m-%d")).alias("_created_at"),
            ])
            
            # Handle specific column
            
            # NOTE: Cot "openfda" nay la mot object, nhung da so truong hop la object rong
            # Theo tieu chi cua Bronze vault nen toi van se giu lai field nay
            # Do chua xac dinh duoc schema chinh xac nen se chuyen sang dang string json
            if "openfda" in df.columns:
                df = df.with_columns(
                    pl.col("openfda").map_elements(
                        lambda x: json.dumps(x) if isinstance(x, dict) or x is not None else "{}",
                        return_dtype=pl.String
                    )
                )

            dfs.append(df)

        else:
            print("[Error] Get OpenFDA failed")
        
    return dfs
