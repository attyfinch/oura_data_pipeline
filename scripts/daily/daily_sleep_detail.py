from config.config import SLEEP_DETAIL_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
from scripts.utils.api_utils import get_oura_data, get_db_connection, OuraAPIError, TokenError, DatabaseError

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

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
    print(f"Fetching sleep detail data from {fourteen_days_ago} to {yesterday}...")

    try:
        sleep_detail_data = get_oura_data(SLEEP_DETAIL_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(sleep_detail_data)

    if df.empty:
        print("⚠️ No sleep detail data available for the specified date range. Skipping.")
        sys.exit(0)

    print("Sample of the data:")
    print(df.head())

    try:
        conn = get_db_connection()

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

        print("✅ Successfully loaded daily sleep detail data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
