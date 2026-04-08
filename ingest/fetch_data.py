import requests
import pandas as pd
import logging
import os
from dotenv import load_dotenv
load_dotenv()
from geopy.geocoders import Nominatim
from datetime import datetime

# --- LOGGER SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("weather_pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
API_KEY = os.getenv("API_KEY")
DEFAULT_CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Mumbai", "New Delhi"]
geolocator = Nominatim(user_agent="weather_monitor")


def get_coords(location_name):
    try:
        location = geolocator.geocode(location_name)
        if location:
            return round(location.latitude, 4), round(location.longitude, 4)
        return None, None
    except Exception as e:
        logger.error(f"Geocoding failed for {location_name}: {e}")
        return None, None


def fetch_weather(locations):
    weather_data = []
    logger.info(f"Starting ingestion for {len(locations)} locations.")

    for loc in locations:
        lat, lon = get_coords(loc)
        if lat and lon:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                weather_data.append({
                    "location_id": hash(loc),  # Simple ID for dbt joining
                    "location_name": loc,
                    "latitude": lat,
                    "longitude": lon,
                    "temp_celsius": data['main']['temp'],
                    "feels_like": data['main']['feels_like'],
                    "humidity": data['main']['humidity'],
                    "weather_main": data['weather'][0]['main'],
                    "ingested_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                logger.info(f"Successfully fetched weather for {loc}")
            except Exception as e:
                logger.error(f"API call failed for {loc}: {e}")
        else:
            logger.warning(f"Skipping {loc} due to missing coordinates.")

    return pd.DataFrame(weather_data)


# --- EXECUTION ---
if __name__ == "__main__":
    # In the future, you can append user_input to this list
    final_df = fetch_weather(DEFAULT_CITIES)

    if not final_df.empty:
        # Save to dbt seeds folder
        seeds_dir=f'{os.path.dirname(os.getcwd())}\\transform\\seeds'
        final_df.to_csv(f"{seeds_dir}\\raw_weather_monitor.csv", index=False)
        logger.info("Pipeline complete. CSV saved to seeds.")

os.path.abspath(__file__)