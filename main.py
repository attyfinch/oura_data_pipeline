import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("OURA_TOKEN")

url = "https://api.ouraring.com/v2/usercollection/daily_readiness"
params = {
    "start_date": "2025-01-01",
    "end_date": "2025-04-20"
}
headers = {
    "Authorization": f"Bearer {token}"
}

response = requests.get(url, headers=headers, params=params)

if response.ok:
    data = response.json()
    
    # Save data to a file in the data directory
    with open("data/oura_readiness_data.json", "w") as f:
        json.dump(data, f, indent=2)

    print("✅ Data saved to oura_readiness_data.json")
else:
    print("❌ Error:", response.status_code, response.text)

