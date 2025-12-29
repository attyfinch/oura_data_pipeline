from config.config import STRESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
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
    print(f"Backfilling stress data from {params['start_date']} to {params['end_date']}...")

    stress_data = get_oura_data(STRESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(stress_data)

    if df.empty:
        print("⚠️ No stress data found for the specified date range.")
        exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW stress_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_stress (
            day,
            day_summary,
            recovery_high,
            stress_high
        )
        SELECT
            CAST(day AS DATE),
            day_summary,
            recovery_high,
            stress_high
        FROM stress_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT day FROM daily_stress
        )
    """)

    print("✅ Successfully backfilled stress data into MotherDuck!")

    conn.close()
