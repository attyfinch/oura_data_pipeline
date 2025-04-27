from config.config import CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN, DEFAULT_START_DATE, DEFAULT_END_DATE
from scripts.utils.api_utils import get_oura_data
from scripts.utils.csv_utils import save_data_to_csv

params = {
    "start_date": DEFAULT_START_DATE,
    "end_date": DEFAULT_END_DATE
}

if __name__ == "__main__":
    print("Fetching cardiovascular age data from Oura...")
    
    cardiovascular_age_data = get_oura_data(CARDIOVASCULAR_AGE_ENDPOINT, OURA_API_TOKEN, params)
    
    df = save_data_to_csv(cardiovascular_age_data, filename="data/cardiovascular_age_backfill.csv")

    print("Sample of the data:")
    print(df.head())
