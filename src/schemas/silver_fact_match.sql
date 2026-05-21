CREATE TABLE IF NOT EXISTS silver_fact_match (
    match_id INTEGER PRIMARY KEY,
    team_h INTEGER NOT NULL,
    team_a INTEGER NOT NULL,
    season VARCHAR NOT NULL,
    pts INTEGER NOT NULL,
    xpts FLOAT NOT NULL
)
USING iceberg
PARTITIONED BY (season);
