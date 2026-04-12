## Project: Atmo Watchtower🌦

An End-to-End Automated Weather Intelligence Pipeline

Atmo Watchtower is a production-grade ELT (Extract, Load, Transform) platform designed to provide high-frequency weather monitoring and safety alerts. Moving beyond static analysis, it integrates live API ingestion with a modern data stack to transform raw atmospheric data into actionable alerts.

<img alt="AtmoWatchTower_ELT.png" height="300" src="artifacts/AtmoWatchTower_ELT.png" width="600"/>

**[Ingestion Layer]** 
1. Python Script (OpenWeatherMap API) fetches real-time JSON data.
2. GeoPy performs reverse-geocoding for precise location mapping.

**[Storage Layer - "Bronze"]** 
3. DuckDB (raw.weather_stream): The data is pushed directly into a local analytical database.

**[Transformation Layer - "Silver & Gold"]** 
4. dbt (Staging): Cleanses data and applies custom Jinja Macros for unit conversions ($C \rightarrow F$).
5. dbt (Marts): Applies business logic to generate alert_levels (e.g., Extreme Heat, Severe Weather).

**[Governance & Quality]** 
6. dbt Tests: Ensures schema integrity (unique IDs, non-null values).
7. Source Freshness: Monitors the "status" of the API to ensure data isn't stale.

**Key Technical Features**
* Modular Transformation: Utilizes dbt Macros to centralize business logic, ensuring that "Extreme Heat" definitions are consistent across all reports.
* Schema Enforcement: Automated data quality testing prevents "garbage" data from reaching the final Marts.
* Warehouse Agnostic Design: While currently running on DuckDB for local speed, the dbt logic is ready to be ported to Snowflake or BigQuery with a single config change.

**How to Run**
1. Clone the Repository and install dependencies using "requirements.txt"
2. Configure Environment: Add your API_KEY to a .env file.
3. Execute the Watchtower:
   ``` python run_pipeline.py```
4. View Results:
   ```dbt docs generate```
   ```dbt docs serve```

**Future Roadmap**
1. Incremental Loading: Optimize for 1-minute intervals without rebuilding full history.
2. UI Dashboard: A live UI to visualize the "Heat Gap" across Indian metros.
3. Webhooks: Automated push notifications when alert levels shift from 'Stable'.