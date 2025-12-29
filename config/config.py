import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env
load_dotenv()

# Access the token file
PROJECT_ROOT = Path(__file__).parent.parent
TOKEN_FILE = PROJECT_ROOT / "secrets" / "oura_tokens.json"

# OAuth2 credentials
OURA_CLIENT_ID = os.getenv("OURA_CLIENT_ID")
OURA_CLIENT_SECRET = os.getenv("OURA_CLIENT_SECRET")

# Base API URL
OURA_BASE_URL = "https://api.ouraring.com/v2/usercollection"

# Endpoints
CARDIOVASCULAR_AGE_ENDPOINT = f"{OURA_BASE_URL}/daily_cardiovascular_age"
ACTIVITY_ENDPOINT = f"{OURA_BASE_URL}/daily_activity"
READINESS_ENDPOINT = f"{OURA_BASE_URL}/daily_readiness"
RESILIENCE_ENDPOINT = f"{OURA_BASE_URL}/daily_resilience"
SLEEP_ENDPOINT = f"{OURA_BASE_URL}/daily_sleep"
SLEEP_DETAIL_ENDPOINT = f"{OURA_BASE_URL}/sleep"
STRESS_ENDPOINT = f"{OURA_BASE_URL}/daily_stress"
SPO2_ENDPOINT = f"{OURA_BASE_URL}/daily_spo2"

# Database settings
# LOCAL_DB_PATH = need to add a database connection here
TABLE_NAME_CARDIOVASCULAR_AGE = "daily_cardiovascular_age"
TABLE_NAME_ACTIVITY = "daily_activity"
TABLE_NAME_READINESS = "daily_readiness"
TABLE_NAME_RESILIENCE = "daily_resilience"
TABLE_NAME_SLEEP = "daily_sleep"
TABLE_NAME_SLEEP_DETAIL = "daily_sleep_detail"
TABLE_NAME_STRESS = "daily_stress"
TABLE_NAME_SPO2 = "daily_spo2"

# This is the start date for data collection from this source
# I purchased a new Oura ring and began tracking my data on 2025-02-20
DEFAULT_START_DATE = "2025-02-20"
