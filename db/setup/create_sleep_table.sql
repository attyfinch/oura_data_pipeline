CREATE TABLE IF NOT EXISTS daily_sleep (
    date DATE PRIMARY KEY,
    sleep_score INTEGER,
    contributors JSON
);