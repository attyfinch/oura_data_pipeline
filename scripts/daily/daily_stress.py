from config.config import STRESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, MOTHERDUCK_TOKEN
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
    print(f"Fetching stress data from {fourteen_days_ago} to {yesterday}...")

    stress_data = get_oura_data(STRESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(stress_data)

    if df.empty:
        print("⚠️ No stress data available for the specified date range. Skipping.")
        exit(0)

    print("Sample of the data:")
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

    print("✅ Successfully loaded daily stress data into MotherDuck!")

    conn.close()
