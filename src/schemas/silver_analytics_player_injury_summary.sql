-- Unpartitioned table mapping enriched metrics for clean player resilience analytics
CREATE TABLE IF NOT EXISTS silver_analytics_player_injury_summary (
    player_id INTEGER NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    injury_duration_days INTEGER NOT NULL,
    cumulative_days_lost INTEGER NOT NULL,
    availability_coefficient FLOAT NOT NULL,
    is_current BOOLEAN NOT NULL
)
USING iceberg;
