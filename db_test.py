import duckdb

# Connect to your MotherDuck database
con = duckdb.connect("md:oura")

# Create a test table (if not exists)
con.execute("""
    CREATE TABLE IF NOT EXISTS oura_test (
        message TEXT
    );
""")

# Insert a test row
con.execute("INSERT INTO oura_test VALUES ('Hello from Python!')")

# Query and print
rows = con.execute("SELECT * FROM oura_test").fetchall()
print("Result from MotherDuck:", rows)
