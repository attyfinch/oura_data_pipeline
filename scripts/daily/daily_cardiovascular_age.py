from config.config import CARDIOVASCULAR_AGE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
from scripts.utils.api_utils import get_oura_data, get_db_connection, OuraAPIError, TokenError, DatabaseError
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

# Set up dates - 14 day lookback
pacific = ZoneInfo("America/Los_Angeles")
now_pacific = datetime.now(pacific)
today = now_pacific.date()
fourteen_days_ago = (now_pacific - timedelta(days=14)).date()

params = {
    "start_date": fourteen_days_ago,
    "end_date": today
}

if __name__ == "__main__":
    print(f"Fetching cardiovascular age data from {fourteen_days_ago} to {today}...")

    try:
        cardiovascular_age_data = get_oura_data(CARDIOVASCULAR_AGE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(cardiovascular_age_data)

    print("Sample of the daily cardiovascular age data:")
    print(df.head())

    try:
        conn = get_db_connection()

        print("Loading daily data into MotherDuck...")

        conn.execute("CREATE OR REPLACE TEMP VIEW cardio_view AS SELECT * FROM df")
        conn.execute(f"""
            INSERT INTO daily_cardiovascular_age (date, cardiovascular_age)
            SELECT
                CAST(day AS DATE),
                vascular_age
            FROM cardio_view
            WHERE CAST(day AS DATE) = DATE '{fourteen_days_ago}'
            AND CAST(day AS DATE) NOT IN (
                SELECT date FROM daily_cardiovascular_age
            )
        """)

        print("✅ Successfully loaded daily cardiovascular age data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
