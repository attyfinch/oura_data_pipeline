from config.config import CARDIOVASCULAR_AGE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET
from scripts.utils.api_utils import get_oura_data
import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Load environment variables
load_dotenv()

# Get MotherDuck token (optional)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set up the date for "yesterday"
pacific = ZoneInfo("America/Los_Angeles")
now_utc = datetime.now(tz=ZoneInfo("UTC"))

# Convert to "now" in Pacific Time (based on UTC runtime)
now_pacific = now_utc.astimezone(pacific)
fourteen_days_ago = (now_pacific - timedelta(days=14)).date()

params = {
    "start_date": fourteen_days_ago,
    "end_date": now_pacific
}

if __name__ == "__main__":  
    print(f"Fetching cardiovascular age data for {fourteen_days_ago}...")

    cardiovascular_age_data = get_oura_data(CARDIOVASCULAR_AGE_ENDPOINT, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
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
        WHERE CAST(day AS DATE) = DATE '{fourteen_days_ago}'
        AND CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_cardiovascular_age
        )
    """)

    print("✅ Successfully loaded daily cardiovascular age data into MotherDuck!")

    conn.close()
