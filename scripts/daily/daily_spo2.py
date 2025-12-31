from config.config import SPO2_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, MOTHERDUCK_TOKEN
from scripts.utils.api_utils import get_oura_data

import duckdb
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
    print(f"Fetching SPO2 data from {fourteen_days_ago} to {yesterday}...")

    spo2_data = get_oura_data(SPO2_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(spo2_data)

    if df.empty:
        print("⚠️ No SPO2 data available for the specified date range. Skipping.")
        exit(0)

    print("Sample of the data:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW spo2_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_spo2 (
            id,
            day,
            breathing_disturbance_index,
            spo2_percentage
        )
        SELECT
            id,
            CAST(day AS DATE),
            breathing_disturbance_index,
            spo2_percentage
        FROM spo2_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT day FROM daily_spo2
        )
    """)

    print("✅ Successfully loaded daily SPO2 data into MotherDuck!")

    conn.close()
