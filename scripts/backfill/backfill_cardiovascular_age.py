from config.config import CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN, DEFAULT_START_DATE
from scripts.utils.api_utils import get_oura_data
from scripts.utils.csv_utils import save_data_to_csv

import duckdb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Grab the MotherDuck token (if available)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# I need to update these params and remove the variables here.
params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": "2025-05-23"
}

if __name__ == "__main__":
    print("Fetching cardiovascular age data from Oura...")
    
    cardiovascular_age_data = get_oura_data(CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN, params)
    
    df = save_data_to_csv(cardiovascular_age_data, filename="data/cardiovascular_age_backfill.csv")

    print("Sample of the data:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    # Insert into MotherDuck
    conn.execute("CREATE OR REPLACE TEMP VIEW cardio_view AS SELECT * FROM df")
    conn.execute("""
        INSERT OR IGNORE INTO daily_cardiovascular_age (
                 date, 
                 cardiovascular_age
                )
        SELECT 
            day AS date, 
            vascular_age AS cardiovascular_age
        FROM cardio_view
    """)

    print("✅ Successfully loaded backfill data into MotherDuck!")

    conn.close()
