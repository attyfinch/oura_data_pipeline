from config.config import RESILIENCE_ENDPOINT, OURA_API_TOKEN, DEFAULT_START_DATE
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
    "end_date": "2025-05-18"
}

if __name__ == "__main__":
    print("Fetching readiness data from Oura...")
    
    resilience_data = get_oura_data(RESILIENCE_ENDPOINT, OURA_API_TOKEN, params)
    
    df = save_data_to_csv(resilience_data, filename="data/resilience_backfill.csv")

    print("Sample of resilience data:")
    print(df.head())

     # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    # # Insert into MotherDuck
    conn.execute("CREATE OR REPLACE TEMP VIEW resilience_view AS SELECT * FROM df")
    conn.execute("""
        INSERT OR IGNORE INTO daily_resilience (
                date,
                contributors,
                level
                )
        SELECT 
            day AS date, 
            contributors,
            level
        FROM resilience_view
    """)

    print("✅ Successfully loaded backfill data into MotherDuck!")

    conn.close()
