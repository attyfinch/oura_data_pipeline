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

# Read the table creation SQL file
with open("db/setup/create_cardiovascular_age_table.sql", "r") as f:
    create_table_sql = f.read()

# Execute the SQL
conn.execute(create_table_sql)

print("✅ Cardiovascular Age table is set up successfully!")

conn.close()
