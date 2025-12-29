CREATE TABLE IF NOT EXISTS daily_spo2 (
    id VARCHAR PRIMARY KEY,
    day DATE,
    breathing_disturbance_index INTEGER,
    spo2_percentage JSON
);