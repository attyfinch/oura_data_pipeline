from config.config import RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE, MOTHERDUCK_TOKEN
from scripts.utils.api_utils import get_oura_data

import duckdb
import pandas as pd

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling resilience data from {params['start_date']} to {params['end_date']}...")

    resilience_data = get_oura_data(RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(resilience_data)

    if df.empty:
        print("⚠️ No resilience data found for the specified date range.")
        exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW resilience_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_resilience (
            date,
            contributors,
            level
        )
        SELECT
            CAST(day AS DATE),
            contributors,
            level
        FROM resilience_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_resilience
        )
    """)

    print("✅ Successfully backfilled resilience data into MotherDuck!")

    conn.close()
