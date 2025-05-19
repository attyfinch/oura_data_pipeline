CREATE TABLE IF NOT EXISTS daily_resilience (
    date DATE PRIMARY KEY,
    contributors JSON,
    level VARCHAR
);