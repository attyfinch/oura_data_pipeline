from config.config import SPO2_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
from scripts.utils.api_utils import get_oura_data

import duckdb
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Grab the MotherDuck token
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling SPO2 data from {params['start_date']} to {params['end_date']}...")

    spo2_data = get_oura_data(SPO2_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(spo2_data)

    if df.empty:
        print("⚠️ No SPO2 data found for the specified date range.")
        exit(0)

    print(f"Found {len(df)} records:")
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

    print("✅ Successfully backfilled SPO2 data into MotherDuck!")

    conn.close()
