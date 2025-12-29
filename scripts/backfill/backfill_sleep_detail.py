from config.config import SLEEP_DETAIL_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
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
    print(f"Backfilling sleep detail data from {params['start_date']} to {params['end_date']}...")

    sleep_detail_data = get_oura_data(SLEEP_DETAIL_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    df = pd.DataFrame(sleep_detail_data)

    if df.empty:
        print("⚠️ No sleep detail data found for the specified date range.")
        exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    conn.execute("CREATE OR REPLACE TEMP VIEW sleep_detail_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_sleep_detail (
            id,
            day,
            average_breath,
            average_heart_rate,
            average_hrv,
            awake_time,
            bedtime_end,
            bedtime_start,
            deep_sleep_duration,
            efficiency,
            latency,
            light_sleep_duration,
            lowest_heart_rate,
            period,
            readiness_score_delta,
            rem_sleep_duration,
            restless_periods,
            sleep_algorithm_version,
            sleep_analysis_reason,
            sleep_score_delta,
            time_in_bed,
            total_sleep_duration,
            type
        )
        SELECT
            id,
            CAST(day AS DATE),
            average_breath,
            average_heart_rate,
            average_hrv,
            awake_time,
            CAST(bedtime_end AS TIMESTAMP),
            CAST(bedtime_start AS TIMESTAMP),
            deep_sleep_duration,
            efficiency,
            latency,
            light_sleep_duration,
            lowest_heart_rate,
            period,
            readiness_score_delta,
            rem_sleep_duration,
            restless_periods,
            sleep_algorithm_version,
            sleep_analysis_reason,
            sleep_score_delta,
            time_in_bed,
            total_sleep_duration,
            type
        FROM sleep_detail_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT day FROM daily_sleep_detail
        )
    """)

    print("✅ Successfully backfilled sleep detail data into MotherDuck!")

    conn.close()
