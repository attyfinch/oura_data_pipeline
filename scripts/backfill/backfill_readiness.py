from config.config import READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE, MOTHERDUCK_TOKEN
from scripts.utils.api_utils import get_oura_data

import duckdb
import pandas as pd

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling readiness data from {params['start_date']} to {params['end_date']}...")

    readiness_data = get_oura_data(READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(readiness_data)

    if df.empty:
        print("⚠️ No readiness data found for the specified date range.")
        exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW readiness_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_readiness (
            date,
            readiness_score,
            temperature_deviation,
            temperature_trend_deviation,
            contributors
        )
        SELECT
            CAST(day AS DATE),
            score,
            temperature_deviation,
            temperature_trend_deviation,
            contributors
        FROM readiness_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_readiness
        )
    """)

    print("✅ Successfully backfilled readiness data into MotherDuck!")

    conn.close()
