from config.config import READINESS_ENDPOINT, OURA_API_TOKEN
from scripts.utils.api_utils import get_oura_data

import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

pacific = ZoneInfo("America/Los_Angeles")

# Load environment variables
load_dotenv()

# Grab the MotherDuck token (if available)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set up the date for "yesterday"
now_pacific = datetime.now(pacific)
yesterday = (now_pacific - timedelta(days=1)).date()

# I need to update these params and remove the variables here.
params = {
    "start_date": yesterday,
    "end_date": yesterday
}

if __name__ == "__main__":
    print("Fetching readiness data from Oura...")
    
    readiness_data = get_oura_data(READINESS_ENDPOINT, OURA_API_TOKEN, params)
    df = pd.DataFrame(readiness_data)

    print("Sample of the data:")
    print(df.head())

     # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    # # Insert into MotherDuck
    conn.execute("CREATE OR REPLACE TEMP VIEW readiness_view AS SELECT * FROM df")
    conn.execute("""
        INSERT OR IGNORE INTO daily_readiness (
                date,
                readiness_score,
                temperature_deviation,
                temperature_trend_deviation,
                contributors
                )
        SELECT 
            day AS date, 
            score AS readiness_score,
            temperature_deviation,
            temperature_trend_deviation,
            contributors
        FROM readiness_view
    """)

    print("✅ Successfully loaded backfill data into MotherDuck!")

    conn.close()
