from config.config import READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
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
    print(f"Fetching readiness data from {fourteen_days_ago} to {yesterday}...")

    try:
        readiness_data = get_oura_data(READINESS_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(readiness_data)

    if df.empty:
        print("⚠️ No readiness data available for the specified date range. Skipping.")
        sys.exit(0)

    print("Sample of the data:")
    print(df.head())

    try:
        conn = get_db_connection()

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

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
