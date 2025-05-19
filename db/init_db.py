import duckdb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Grab the MotherDuck token (if available)
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

# Connect depending on whether a token is present
if MOTHERDUCK_TOKEN:
    conn = duckdb.connect(f"md:oura?motherduck_token={MOTHERDUCK_TOKEN}")
else:
    conn = duckdb.connect("md:oura")

# List of SQL setup files to run
sql_files = [
    "db/setup/create_cardiovascular_age_table.sql",
    "db/setup/create_activity_table.sql"
]

# Execute each SQL file
for path in sql_files:
    with open(path, "r") as f:
        conn.execute(f.read())
    print(f"✅ Executed: {path}")

conn.close()
print("✅ All setup complete!")
