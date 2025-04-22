import os
import requests
from dotenv import load_dotenv

# Load token from .env
load_dotenv()
token = os.getenv("OURA_TOKEN")

# Oura API endpoint: Daily Readiness (you can swap for sleep, activity, etc.)
url = "https://api.ouraring.com/v2/usercollection/daily_readiness"

# Example query: last 7 days
params = {
    "start_date": "2025-04-14",
    "end_date": "2025-04-20"
}

headers = {
    "Authorization": f"Bearer {token}"
}

response = requests.get(url, headers=headers, params=params)

# Pretty print the result
if response.ok:
    data = response.json()
    print("✅ Success! Here's what we got:")
    print(data)
else:
    print("❌ Failed to fetch data:")
    print(response.status_code, response.text)
