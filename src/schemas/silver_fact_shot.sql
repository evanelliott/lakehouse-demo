CREATE TABLE IF NOT EXISTS silver_fact_shot (
    id VARCHAR PRIMARY KEY,
    player_id INTEGER NOT NULL,
    match_id INTEGER NOT NULL,
    xg FLOAT NOT NULL
)
USING iceberg
PARTITIONED BY (bucket(100, match_id));
