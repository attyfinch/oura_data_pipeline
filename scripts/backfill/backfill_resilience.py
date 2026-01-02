from config.config import RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, DEFAULT_START_DATE
from scripts.utils.api_utils import get_oura_data, get_db_connection, OuraAPIError, TokenError, DatabaseError

import pandas as pd
import sys

# Set your backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling resilience data from {params['start_date']} to {params['end_date']}...")

    try:
        resilience_data = get_oura_data(RESILIENCE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    except TokenError as e:
        print(f"❌ Token error: {e}")
        sys.exit(1)
    except OuraAPIError as e:
        print(f"❌ API error: {e}")
        sys.exit(1)
    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)

    df = pd.DataFrame(resilience_data)

    if df.empty:
        print("⚠️ No resilience data found for the specified date range.")
        sys.exit(0)

    print(f"Found {len(df)} records:")
    print(df.head())

    try:
        conn = get_db_connection()

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

        print("✅ Successfully backfilled resilience data into MotherDuck!")
        conn.close()

    except DatabaseError as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
