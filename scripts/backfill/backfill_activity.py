from config.config import ACTIVITY_ENDPOINT, OURA_API_TOKEN, DEFAULT_START_DATE
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
    "end_date": "2025-05-17"
}

if __name__ == "__main__":
    print("Fetching cardiovascular age data from Oura...")
    
    activity_data = get_oura_data(ACTIVITY_ENDPOINT, OURA_API_TOKEN, params)
    
    df = save_data_to_csv(activity_data, filename="data/activity_backfill.csv")

    print("Sample of the data:")
    print(df.head())

    # Connect to MotherDuck
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        conn = duckdb.connect("md:oura")

    print("Loading data into MotherDuck...")

    # # Insert into MotherDuck
    conn.execute("CREATE OR REPLACE TEMP VIEW activity_view AS SELECT * FROM df")
    conn.execute("""
        INSERT OR IGNORE INTO daily_activity (
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
            day AS date, 
            score AS activity_score,
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
    """)

    print("✅ Successfully loaded backfill data into MotherDuck!")

    conn.close()
