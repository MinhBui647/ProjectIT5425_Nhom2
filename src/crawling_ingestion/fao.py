import io
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import polars as pl

from crawling_ingestion.utils import datetime_validate_range, datetime_now_utc


FAO_BASE_API = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
FAO_DATA_LINK_INNERTEXT = "Excel: Nominal and real indices from 1990 onwards (monthly and annual)"
SHEET_INDEX = 3  # sheet thu 3 (indices_monthly_real) 



def _get_and_process_fao_sheet_data(download_link: str):
    file_res = requests.get(download_link, timeout=10)
            
    if file_res.status_code == 200:
        # Doc file excel
        raw_data_stream = io.BytesIO(file_res.content)
        df = pl.read_excel(raw_data_stream, sheet_id=3)
        
        # Lay ten cot (dong thu 1?) va bo cot dau tien (Year)
        header_row = df.row(1)
        headers = header_row[1:]
        df = df.drop(df.columns[0])
        
        # Bo may dong dau tien (chu thich linh tinh + ten cot)
        df = df.slice(2)

        # Update ten cot cua dataframe
        rename_dict = { old: new for old, new in zip(df.columns, headers)}
        df = df.rename(rename_dict)
        
        return df
    
    print("[Error] Failed to download sheet")
    return None



def get_fao_marketindices_data(
    start_date: str,
    end_date: str,
):
    """
        Params:
            start_date: str, format "YYYY-mm-dd"
            end_date: str, format "YYYY-mm-dd"
        
        Returns:
            Dict | None
    """
    
    _s, _e = datetime_validate_range(start_date, end_date)
    
    res = requests.get(FAO_BASE_API, timeout=10)
    
    if res.status_code == 200:
        soup = BeautifulSoup(res.text, 'html.parser')
        link_element = soup.find('a', string=FAO_DATA_LINK_INNERTEXT)
        
        if not link_element:
            print("[Error] Failed to download file")
            return None
        
        download_link = link_element.get("href")
        df = _get_and_process_fao_sheet_data(download_link)
        
        if df is not None:
            # Lay data trong khoang start - end date
            ranged_df = df.filter(pl.col("Month").is_between(pl.lit(start_date), pl.lit(end_date)))

            # Them field de track
            ranged_df = ranged_df.with_columns([
                pl.lit(FAO_BASE_API).alias("_base_api"),
                pl.lit(datetime_now_utc()).alias("_created_at"),
            ])

            return ranged_df
    
    return None
    
