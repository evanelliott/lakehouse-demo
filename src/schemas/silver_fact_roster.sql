CREATE TABLE IF NOT EXISTS silver_fact_roster (
    player_id INTEGER NOT NULL,
    match_id INTEGER NOT NULL,
    PRIMARY KEY (player_id, match_id)
)
USING iceberg
PARTITIONED BY (match_id);
