-- View: clean.activity
-- Source: main.oura_daily_activity
-- Description: Transformed daily activity data with durations in hours/minutes

CREATE OR REPLACE VIEW clean.activity AS
SELECT
    date,
    
    -- Score
    activity_score,
    
    -- Calories
    active_calories,
    target_calories,
    total_calories,
    
    -- Steps and distance
    steps,
    equivalent_walking_distance,
    meters_to_target,
    target_meters,
    
    -- Activity times in hours
    high_activity_time / 3600.0 AS high_activity_hours,
    medium_activity_time / 3600.0 AS medium_activity_hours,
    low_activity_time / 3600.0 AS low_activity_hours,
    sedentary_time / 3600.0 AS sedentary_hours,
    resting_time / 3600.0 AS resting_hours,
    non_wear_time / 3600.0 AS non_wear_hours,
    
    -- Activity times in minutes
    high_activity_time / 60.0 AS high_activity_mins,
    medium_activity_time / 60.0 AS medium_activity_mins,
    low_activity_time / 60.0 AS low_activity_mins,
    sedentary_time / 60.0 AS sedentary_mins,
    resting_time / 60.0 AS resting_mins,
    non_wear_time / 60.0 AS non_wear_mins,
    
    -- MET minutes
    average_met_minutes,
    high_activity_met_minutes,
    medium_activity_met_minutes,
    low_activity_met_minutes,
    sedentary_met_minutes,
    
    -- Other
    inactivity_alerts

FROM oura.main.daily_activity