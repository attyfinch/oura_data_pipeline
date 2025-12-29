from config.config import (
    CARDIOVASCULAR_AGE_ENDPOINT, 
    OURA_CLIENT_ID, 
    OURA_CLIENT_SECRET, 
    DEFAULT_START_DATE
)
from scripts.utils.api_utils import get_oura_data
import duckdb
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Grab MotherDuck token
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Set backfill date range
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-12-28"  # Update this to your desired end date
}

if __name__ == "__main__":
    print(f"Backfilling cardiovascular age data from {params['start_date']} to {params['end_date']}...")
    
    cardiovascular_age_data = get_oura_data(
        CARDIOVASCULAR_AGE_ENDPOINT, 
        OURA_CLIENT_ID, 
        OURA_CLIENT_SECRET, 
        params
    )
    
    df = pd.DataFrame(cardiovascular_age_data)
    
    if df.empty:
        print("⚠️ No cardiovascular age data found for the specified date range.")
        exit(0)
    
    print(f"Found {len(df)} records:")
    print(df.head())
    
    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")
    
    print("Loading data into MotherDuck...")
    
    conn.execute("CREATE OR REPLACE TEMP VIEW cardio_view AS SELECT * FROM df")
    conn.execute("""
        INSERT INTO daily_cardiovascular_age (date, cardiovascular_age)
        SELECT
            CAST(day AS DATE),
            vascular_age
        FROM cardio_view
        WHERE CAST(day AS DATE) NOT IN (
            SELECT date FROM daily_cardiovascular_age
        )
    """)
    
    print("✅ Successfully backfilled cardiovascular age data into MotherDuck!")
    
    conn.close()