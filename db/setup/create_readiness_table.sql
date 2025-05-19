CREATE TABLE IF NOT EXISTS daily_readiness (
    date DATE PRIMARY KEY,
    readiness_score INTEGER,
    temperature_deviation DOUBLE,
    temperature_trend_deviation DOUBLE,
    contributors JSON
);