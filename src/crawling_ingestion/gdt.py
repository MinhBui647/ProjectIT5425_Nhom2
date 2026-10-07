import math
import json
import polars as pl
import requests
from datetime import datetime


GDT_BASE_API = f"https://s3.amazonaws.com/www-production.globaldairytrade.info/results"
GDT_EVENT_DATE_FORMAT = "%B %d, %Y %H:%M:%S"
GDT_PRICE_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"



def _get_gdt_base(endpoint: str):
    api = f"{GDT_BASE_API}/{endpoint}"
    res = requests.get(api, timeout=10)
    
    if res.status_code == 200:
        data = res.json()
        return data
    else:
        print("[Error] Failed to fetch GDT API")

    return None
    


def _get_gdt_latest_event_guid():
    data = _get_gdt_base("latest.json")
    if data is not None:
        return data["latestEvent"]
    return None

def _get_gdt_event_summary(event_guid: str):
    data = _get_gdt_base(f"{event_guid}/event_summary.json")
    if data is not None:
        return data["EventSummary"]
    return None
    
def _get_gdt_latest_events(latest_event_guid: str):
    data = _get_gdt_base(f"{latest_event_guid}/price_indices_ten_years.json")
    if data is not None:
        return data["PriceIndicesTenYears"]["Events"]["EventDetails"]
    return None



def get_gdt_events(start_date: str, end_date: str,):
    """
    Params:
        start_date: str, format "YYYY-mm-dd"
        end_date: str, format "YYYY-mm-dd"
    
    Returns:
        tuple: (events, event_checkpoint_guids)
    """
    
    dt_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end_date = datetime.strptime(end_date, "%Y-%m-%d")
    events = []
    event_checkpoint_guids = []
    isOlder = True # Check if need to searching older events
    
    # Get latest event
    latest_event_guid = _get_gdt_latest_event_guid()
    event_checkpoint_guids.append(latest_event_guid)
    
    while isOlder:
        isOlder = False
        
        # Get latest 10 year events
        latest_10y_events = _get_gdt_latest_events(latest_event_guid)
        
        if latest_10y_events is None:
            break
        
        # Take events in defined range
        for ev in latest_10y_events[::-1]:
            ev_date = datetime.strptime(ev["EventDate"], GDT_EVENT_DATE_FORMAT)
            if dt_start_date <= ev_date <= dt_end_date:
                ev_summary = _get_gdt_event_summary(ev["EventGUID"])
                events.append(ev_summary)
        
        # Get oldest event of the 10y and compare with the start date
        oldest_event = latest_10y_events[0]
        oldest_event_date = datetime.strptime(oldest_event["EventDate"], GDT_EVENT_DATE_FORMAT)

        # Check if need older events depends on the start date
        if oldest_event_date > dt_start_date:
            latest_event_guid = oldest_event["EventGUID"]
            event_checkpoint_guids.append(latest_event_guid)
            isOlder = True
        
    return events, event_checkpoint_guids



def _get_gdt_product_summaries(event_guid: str):
    data = _get_gdt_base(f"{event_guid}/product_groups_summary.json")
    if data is not None:
        return data["ProductGroups"]["ProductGroupResult"]
    return None

def _get_gdt_latest_winning_price(latest_event_guid: str, product_code: str):
    data = _get_gdt_base(f"{latest_event_guid}/product_group_winning_prices_5_years_{product_code}.json")
    if data is not None:
        return data["ProductGroup"]["Events"]["Event"]
    return None





def get_gdt_marketprice_data(
    start_date: str,
    end_date: str,
    product_codes: list[str] = ["AMF", "SMP", "WMP"],
):
    """
    Params:
        start_date: str, format "YYYY-mm-dd"
        end_date: str, format "YYYY-mm-dd"
        product_codes: list[str]
    
    Returns:
        Dict | None
    """
    
    dt_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end_date = datetime.strptime(end_date, "%Y-%m-%d")
    winning_prices = []

    # Get latest event
    latest_event_guid = _get_gdt_latest_event_guid()
    
    # Get all available products
    available_products = _get_gdt_product_summaries(latest_event_guid)
    available_product_codes = [prod["ProductGroupCode"] for prod in available_products]
    
    for prod_code in product_codes:
        if prod_code not in available_product_codes:
            print(f"[Error] Product code {prod_code} is not available")
            continue
        
        # Check if need to searching older events
        isOlder = True 
        
        current_event_guid = latest_event_guid
        
        while isOlder:
            isOlder = False
            
            # Get latest 5 year winning prices
            latest_5y_price = _get_gdt_latest_winning_price(current_event_guid, prod_code)
            if latest_5y_price is None:
                print(f"[Error] Failed to get winning price of product code {prod_code}")
                break
            
            # Take price which its event date is in a defined range
            for price_event in latest_5y_price[::-1]:
                event_date = datetime.strptime(price_event["EventDate"], GDT_PRICE_DATE_FORMAT)

                if dt_start_date <= event_date <= dt_end_date:
                    price_event["ProductGroupCode"] = prod_code
                    winning_prices.append(price_event)
            
            # Get oldest price of the 5y and compare with the start date
            oldest_price_event = latest_5y_price[0]
            oldest_price_event_date = datetime.strptime(oldest_price_event["EventDate"], GDT_PRICE_DATE_FORMAT)

            # Check if need older events depends on the start date
            if oldest_price_event_date > dt_start_date:
                isOlder = True
                event_number = oldest_price_event["EventNumber"]
                
                # Find next checkpoint event guid
                latest_10y_events = _get_gdt_latest_events(current_event_guid)
                for ev in latest_10y_events:
                    if event_number == ev["EventNumber"]:
                        current_event_guid = ev["EventGUID"]
                        break
        
    df = pl.DataFrame(winning_prices)
    df = df.with_columns([
        pl.lit(datetime.now().strftime("%Y-%m-%d")).alias("_created_at"),
    ])
    
    return df

