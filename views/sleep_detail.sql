CREATE OR REPLACE VIEW clean.sleep_detail AS
SELECT
    day as date,
    bedtime_start,
    bedtime_end,
    
    -- Total sleep in both units
    total_sleep_duration / 3600.0 AS total_sleep_hours,
    total_sleep_duration / 60.0 AS total_sleep_mins,
    
    -- Sleep stages in hours
    deep_sleep_duration / 3600.0 AS deep_sleep_hours,
    rem_sleep_duration / 3600.0 AS rem_sleep_hours,
    light_sleep_duration / 3600.0 AS light_sleep_hours,
    awake_time / 3600.0 AS awake_hours,
    time_in_bed / 3600.0 AS time_in_bed_hours,
    
    -- Sleep stages in minutes
    deep_sleep_duration / 60.0 AS deep_sleep_mins,
    rem_sleep_duration / 60.0 AS rem_sleep_mins,
    light_sleep_duration / 60.0 AS light_sleep_mins,
    awake_time / 60.0 AS awake_mins,
    time_in_bed / 60.0 AS time_in_bed_mins,
    
    latency / 60.0 AS latency_mins,
    
    -- Scores and vitals
    efficiency,
    restless_periods,
    average_hrv,
    average_heart_rate,
    lowest_heart_rate,
    average_breath
    
FROM oura.main.daily_sleep_detail
WHERE total_sleep_duration >= 7200  -- Filter out naps/small sessions (< 2 hours)