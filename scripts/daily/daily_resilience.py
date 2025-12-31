from config.config import RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, MOTHERDUCK_TOKEN
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
    print(f"Fetching resilience data from {fourteen_days_ago} to {yesterday}...")

    resilience_data = get_oura_data(RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(resilience_data)

    if df.empty:
        print("⚠️ No resilience data available for the specified date range. Skipping.")
        exit(0)

    print("Sample of the data:")
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

    print("✅ Successfully loaded daily resilience data into MotherDuck!")

    conn.close()
