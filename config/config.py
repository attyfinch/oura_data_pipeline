import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# API Authentication
OURA_API_TOKEN = os.getenv("OURA_API_TOKEN")

# Base API URL
OURA_BASE_URL = "https://api.ouraring.com/v2/usercollection"

# Endpoints 
CARDIOVASCULAR_AGE_ENDPOINT = f"{OURA_BASE_URL}/daily_cardiovascular_age"
ACTIVITY_ENDPOINT = f"{OURA_BASE_URL}/daily_activity"
READINESS_ENDPOINT = f"{OURA_BASE_URL}/daily_readiness"
RESILIENCE_ENDPOINT = f"{OURA_BASE_URL}/daily_resilience"

# Database settings
# LOCAL_DB_PATH = need to add a database connection here
TABLE_NAME_CARDIOVASCULAR_AGE = "daily_cardiovascular_age"
TABLE_NAME_ACTIVITY = "daily_activity"
TABLE_NAME_READINESS = "daily_readiness"
TABLE_NAME_RESILIENCE = "daily_resilience"

# This is the start date for data collection from this source
# I purchased a new Oura ring and began tracking my data on 2025-02-20
DEFAULT_START_DATE = "2025-02-20"
