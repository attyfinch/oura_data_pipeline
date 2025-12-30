import json
import duckdb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Grab the MotherDuck token
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


# Read current tokens from your secrets file
with open("secrets/oura_tokens.json") as f:
    tokens = json.load(f)

# Connect and create table + seed
conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")

conn.execute("""
    CREATE TABLE IF NOT EXISTS auth_tokens (
        service VARCHAR PRIMARY KEY,
        access_token VARCHAR NOT NULL,
        refresh_token VARCHAR NOT NULL,
        expires_at VARCHAR NOT NULL,
        updated_at VARCHAR NOT NULL
    )
""")

conn.execute("""
    INSERT INTO auth_tokens (service, access_token, refresh_token, expires_at, updated_at)
    VALUES ('oura', ?, ?, ?, CURRENT_TIMESTAMP)
""", [tokens["access_token"], tokens["refresh_token"], tokens["expires_at"]])

print("Token table created and seeded!")
conn.close()