import requests
import pandas as pd
import logging
import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

from geopy.geocoders import Nominatim
from datetime import datetime

# --- LOGGER SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("logs/weather_pipeline.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
API_KEY = os.getenv("API_KEY")
DEFAULT_CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Mumbai", "New Delhi"]
geolocator = Nominatim(user_agent="weather_monitor")

# Database Path - Points to the .duckdb file in your data folder
PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJ_ROOT, "data", "weather_data.duckdb")


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
            # Using current_time for accurate metadata
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()

                weather_data.append({
                    "location_id": abs(hash(loc)) % (10 ** 8),  # Consistent 8-digit ID
                    "location_name": loc,
                    "latitude": lat,
                    "longitude": lon,
                    "temp_celsius": data['main']['temp'],
                    "feels_like": data['main']['feels_like'],
                    "humidity": data['main']['humidity'],
                    "weather_main": data['weather'][0]['main'],
                    "recorded_at": datetime.now()
                })
                logger.info(f"Successfully fetched weather for {loc}")
            except Exception as e:
                logger.error(f"API call failed for {loc}: {e}")
        else:
            logger.warning(f"Skipping {loc} due to missing coordinates.")

    return pd.DataFrame(weather_data)


# --- NEW: DUCKDB LOAD FUNCTION ---
def load_to_duckdb(df):
    try:
        # Connect to the database
        con = duckdb.connect(DB_PATH)

        # Create 'bronze' schema for organization
        con.execute("CREATE SCHEMA IF NOT EXISTS BRONZE")

        # Load the DataFrame directly into a DuckDB table
        # We use 'CREATE OR REPLACE' to keep it fresh, but you could use 'INSERT' for history
        con.execute("CREATE OR REPLACE TABLE BRONZE.WEATHER_STREAM AS SELECT * FROM df")

        logger.info(f"Successfully pushed {len(df)} rows to DuckDB at {DB_PATH}")
        con.close()
    except Exception as e:
        logger.error(f"Failed to load to DuckDB: {e}")


# --- EXECUTION ---
if __name__ == "__main__":
    final_df = fetch_weather(DEFAULT_CITIES)

    if not final_df.empty:
        # Pushing to Database instead of CSV
        load_to_duckdb(final_df)
        logger.info("Pipeline complete. Live data is now in the warehouse.")
    else:
        logger.warning("No data fetched. Check API key or connection.")