from config.config import READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, MOTHERDUCK_TOKEN
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
    print(f"Fetching readiness data from {fourteen_days_ago} to {yesterday}...")

    readiness_data = get_oura_data(READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(readiness_data)

    if df.empty:
        print("⚠️ No readiness data available for the specified date range. Skipping.")
        exit(0)

    print("Sample of the data:")
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

    print("✅ Successfully loaded daily readiness data into MotherDuck!")

    conn.close()
