from config.config import SLEEP_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
from scripts.utils.api_utils import get_oura_data

import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Grab the MotherDuck token
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set up dates - 14 day lookback
pacific = ZoneInfo("America/Los_Angeles")
now_pacific = datetime.now(pacific)
yesterday = (now_pacific - timedelta(days=1)).date()
fourteen_days_ago = (now_pacific - timedelta(days=14)).date()

params = {
    "start_date": fourteen_days_ago,
    "end_date": yesterday
}

if __name__ == "__main__":
    print(f"Fetching sleep data from {fourteen_days_ago} to {yesterday}...")

    sleep_data = get_oura_data(SLEEP_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(sleep_data)

    if df.empty:
        print("⚠️ No sleep data available for the specified date range. Skipping.")
        exit(0)

    print("Sample of the data:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW sleep_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_sleep (
            date,
            sleep_score,
            contributors
        )
        SELECT
            CAST(day AS DATE),
            score,
            contributors
        FROM sleep_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_sleep
        )
    """)

    print("✅ Successfully loaded daily sleep data into MotherDuck!")

    conn.close()
