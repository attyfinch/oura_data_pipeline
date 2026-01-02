from config.config import ACTIVITY_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
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
    print(f"Fetching activity data from {fourteen_days_ago} to {yesterday}...")

    try:
        activity_data = get_oura_data(ACTIVITY_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(activity_data)

    if df.empty:
        print("⚠️ No activity data available for the specified date range. Skipping.")
        sys.exit(0)

    print("Sample of the data:")
    print(df.head())

    try:
        conn = get_db_connection()

        print("Loading data into MotherDuck...")

        conn.execute("CREATE OR REPLACE TEMP VIEW activity_view AS SELECT * FROM df")
        conn.execute("""
            INSERT INTO daily_activity (
                date,
                activity_score,
                active_calories,
                average_met_minutes,
                contributors,
                equivalent_walking_distance,
                high_activity_met_minutes,
                high_activity_time,
                inactivity_alerts,
                low_activity_met_minutes,
                low_activity_time,
                medium_activity_met_minutes,
                medium_activity_time,
                meters_to_target,
                non_wear_time,
                resting_time,
                sedentary_met_minutes,
                sedentary_time,
                steps,
                target_calories,
                target_meters,
                total_calories
            )
            SELECT
                CAST(day AS DATE),
                score,
                active_calories,
                average_met_minutes,
                contributors,
                equivalent_walking_distance,
                high_activity_met_minutes,
                high_activity_time,
                inactivity_alerts,
                low_activity_met_minutes,
                low_activity_time,
                medium_activity_met_minutes,
                medium_activity_time,
                meters_to_target,
                non_wear_time,
                resting_time,
                sedentary_met_minutes,
                sedentary_time,
                steps,
                target_calories,
                target_meters,
                total_calories
            FROM activity_view
            WHERE CAST(day AS DATE) NOT IN (
                SELECT date FROM daily_activity
            )
        """)

        print("✅ Successfully loaded daily activity data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)