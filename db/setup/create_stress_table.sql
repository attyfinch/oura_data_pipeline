CREATE TABLE IF NOT EXISTS daily_stress (
    day DATE PRIMARY KEY,
    day_summary VARCHAR,
    recovery_high INTEGER,
    stress_high INTEGER
);