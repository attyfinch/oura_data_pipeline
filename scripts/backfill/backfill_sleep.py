from config.config import SLEEP_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
from scripts.utils.api_utils import get_oura_data, get_db_connection, OuraAPIError, TokenError, DatabaseError

import pandas as pd
import sys

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling sleep data from {params['start_date']} to {params['end_date']}...")

    try:
        sleep_data = get_oura_data(SLEEP_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(sleep_data)

    if df.empty:
        print("⚠️ No sleep data found for the specified date range.")
        sys.exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    try:
        conn = get_db_connection()

        print("Loading data into MotherDuck...")

        conn.execute("CREATE OR REPLACE TEMP VIEW sleep_view AS SELECT * FROM df")
        conn.execute("""
            INSERT INTO daily_sleep (
                date,
                sleep_score,
                contributors
            )
            SELECT
                CAST(day AS DATE),
                score as sleep_score,
                contributors
            FROM sleep_view
            WHERE CAST(day AS DATE) NOT IN (
                SELECT date FROM daily_sleep
            )
        """)

        print("✅ Successfully backfilled sleep data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
