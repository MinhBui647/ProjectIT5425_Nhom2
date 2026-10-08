import polars as pl
import requests

from crawling_ingestion.utils import datetime_validate_range, datetime_now_utc

OPENMETEO_BASE_API = "https://archive-api.open-meteo.com/v1/archive"

def get_openmeteo_weather_data(
    farm_id: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    params: list[str] = ["temperature_2m", "relative_humidity_2m", "precipitation"],
    timezone: str = "UTC",
    models: str = "era5",
    temperature_unit: str = "celsius",
    precipitation_unit: str = "mm",
):
    """
    Params:
        latitude: float
        longitude: float
        start_date: str (ISO format: yyyy-mm-dd)
        end_date: str (ISO format: yyyy-mm-dd)
        params: str[]
        timezone: str
        models: str
        temperature_unit: str
        precipitation_unit: str
    
    Returns:
        Dict | None
    """
    
    _s, _e = datetime_validate_range(start_date, end_date)
    
    openmeteo_api = f"{OPENMETEO_BASE_API}?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={",".join(params)}&timezone={timezone}&models={models}&temperature_unit={temperature_unit}&precipitation_unit={precipitation_unit}"
    res = requests.get(openmeteo_api, timeout=15)
    
    # Get successfully
    if res.status_code == 200:
        data = res.json()["hourly"]
        
        # Format example: {"time":"2026-09-22T00:00","temperature_2m":19.6,"relative_humidity_2m":95,"precipitation":0}
        data = pl.DataFrame({
            "farm_id": farm_id,
            "observed_at": data["time"],
            f"temperature_{temperature_unit}": data["temperature_2m"],
            "relative_humidity_pct": data["relative_humidity_2m"],
            f"precipitation_{precipitation_unit}": data["precipitation"],

            "_base_api": OPENMETEO_BASE_API,
            "_created_at": datetime_now_utc(),
        })
        
        return data

    return None

