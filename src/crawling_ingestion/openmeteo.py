import polars as pl
import requests
from datetime import datetime

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
    
    openmeteo_api = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly={",".join(params)}&timezone={timezone}&models={models}&temperature_unit={temperature_unit}&precipitation_unit={precipitation_unit}"
    res = requests.get(openmeteo_api, timeout=15)
    
    print(f"OpenMeteo API: {openmeteo_api}")
    
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
            "_created_at": datetime.now().strftime("%Y-%m-%d"),
        })
        
        return data

    return None

