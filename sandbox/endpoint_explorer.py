"""
Oura API Explorer - Sandbox module for exploring endpoint data
Easily swap between endpoints to inspect data structure before building schemas.

Usage:
    python oura_explorer.py                     # defaults to daily_sleep, prints JSON
    python oura_explorer.py --endpoint sleep    # explore sleep periods
    python oura_explorer.py --csv               # save to CSV instead of printing
    python oura_explorer.py --days 7            # change lookback period
"""

from scripts.utils.api_utils import get_oura_data
from config.config import OURA_CLIENT_ID, OURA_CLIENT_SECRET

import argparse
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# =============================================================================
# ENDPOINT CONFIGURATION
# =============================================================================
# Base URL for Oura API v2
BASE_URL = "https://api.ouraring.com/v2/usercollection"

# Available endpoints to explore
ENDPOINTS = {
    "daily_sleep": f"{BASE_URL}/daily_sleep",
    "daily_stress": f"{BASE_URL}/daily_stress",
    "daily_spo2": f"{BASE_URL}/daily_spo2",
    "sleep": f"{BASE_URL}/sleep",  # sleep periods (detailed)
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_date_range(days_back: int = 14) -> tuple[str, str]:
    """Get start and end dates for the API request."""
    pacific = ZoneInfo("America/Los_Angeles")
    now_pacific = datetime.now(pacific)
    end_date = (now_pacific - timedelta(days=1)).date()
    start_date = (now_pacific - timedelta(days=days_back)).date()
    return str(start_date), str(end_date)


def fetch_endpoint_data(endpoint_key: str, days_back: int = 14) -> list[dict]:
    """Fetch data from the specified Oura endpoint."""
    if endpoint_key not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_key}. Available: {list(ENDPOINTS.keys())}")
    
    endpoint_url = ENDPOINTS[endpoint_key]
    start_date, end_date = get_date_range(days_back)
    
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    print(f"Fetching {endpoint_key} data from {start_date} to {end_date}...")
    print(f"Endpoint: {endpoint_url}")
    print("-" * 60)
    
    data = get_oura_data(endpoint_url, OURA_CLIENT_ID, OURA_CLIENT_SECRET, params)
    return data


def print_json(data: list[dict], sample_size: int = 2) -> None:
    """Pretty print JSON data to terminal."""
    if not data:
        print("⚠️ No data returned from API")
        return
    
    print(f"\n📊 Total records: {len(data)}")
    print(f"📋 Showing first {min(sample_size, len(data))} record(s):\n")
    
    sample = data[:sample_size]
    print(json.dumps(sample, indent=2, default=str))
    
    # Show field summary
    if data:
        print("\n" + "=" * 60)
        print("📝 FIELD SUMMARY (top-level keys):")
        print("=" * 60)
        for key in data[0].keys():
            value = data[0][key]
            value_type = type(value).__name__
            if isinstance(value, dict):
                nested_keys = list(value.keys())
                print(f"  • {key}: {value_type} → {nested_keys}")
            elif isinstance(value, list) and value:
                inner_type = type(value[0]).__name__
                print(f"  • {key}: list[{inner_type}] (len={len(value)})")
            else:
                print(f"  • {key}: {value_type}")


def save_to_csv(data: list[dict], endpoint_key: str, output_dir: str = "csv_output") -> str:
    """Save data to CSV file, flattening nested structures."""
    if not data:
        print("⚠️ No data to save")
        return ""
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Use pandas json_normalize to flatten nested dicts
    df = pd.json_normalize(data)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{endpoint_key}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False)
    
    print(f"\n✅ Saved {len(df)} records to: {filepath}")
    print(f"📋 Columns ({len(df.columns)}):")
    for col in df.columns:
        print(f"   • {col}")
    
    return filepath


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Explore Oura API endpoints")
    parser.add_argument(
        "--endpoint", "-e",
        choices=list(ENDPOINTS.keys()),
        default="daily_sleep",
        help="Which endpoint to query (default: daily_sleep)"
    )
    parser.add_argument(
        "--csv", "-c",
        action="store_true",
        help="Save to CSV instead of printing JSON"
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=14,
        help="Number of days to look back (default: 14)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="csv_output",
        help="Output directory for CSV files (default: csv_output)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"🔍 OURA API EXPLORER")
    print("=" * 60)
    
    # Fetch data
    data = fetch_endpoint_data(args.endpoint, args.days)
    
    # Output
    if args.csv:
        save_to_csv(data, args.endpoint, args.output_dir)
    else:
        print_json(data)
    
    print("\n" + "=" * 60)
    print("Available endpoints: " + ", ".join(ENDPOINTS.keys()))
    print("=" * 60)


if __name__ == "__main__":
    main()