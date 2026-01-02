from config.config import ACTIVITY_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
from scripts.utils.api_utils import get_oura_data, get_db_connection, OuraAPIError, TokenError, DatabaseError

import pandas as pd
import sys

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling activity data from {params['start_date']} to {params['end_date']}...")

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
        print("⚠️ No activity data found for the specified date range.")
        sys.exit(0)

    print(f"Found {len(df)} records:")
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

        print("✅ Successfully backfilled activity data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)