from config.config import CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN
from scripts.utils.api_utils import get_oura_data

import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

pacific = ZoneInfo("America/Los_Angeles")

# Load environment variables
load_dotenv()

# Get MotherDuck token (optional)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set up the date for "yesterday"
now_pacific = datetime.now(pacific)
yesterday = (now_pacific - timedelta(days=1)).date()

params = {
    "start_date": yesterday,
    "end_date": yesterday
}

if __name__ == "__main__":
    print(f"Fetching cardiovascular age data for {yesterday}...")

    cardiovascular_age_data = get_oura_data(CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN, params)
    df = pd.DataFrame(cardiovascular_age_data)

    print("Sample of the daily cardiovascular age data:")
    print(df.head())

    # Connect to MotherDuck (oura DB)
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading daily data into MotherDuck...")

    # Create temp view and insert into table
    conn.execute("CREATE OR REPLACE TEMP VIEW cardio_view AS SELECT * FROM df")
    conn.execute(f"""
        INSERT INTO daily_cardiovascular_age (date, cardiovascular_age)
        SELECT
            CAST(day AS DATE),
            vascular_age
        FROM cardio_view
        WHERE CAST(day AS DATE) = DATE '{yesterday}'
        AND CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_cardiovascular_age
        )
    """)

    print("✅ Successfully loaded daily cardiovascular age data into MotherDuck!")

    conn.close()
