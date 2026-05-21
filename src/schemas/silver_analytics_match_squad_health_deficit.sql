CREATE TABLE IF NOT EXISTS silver_analytics_match_squad_health_deficit (
    match_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    active_injured_count INTEGER NOT NULL,
    total_squad_xg_drag FLOAT NOT NULL,
    total_squad_xga_drag FLOAT NOT NULL,
    PRIMARY KEY (match_id, team_id)
)
USING iceberg
PARTITIONED BY (match_id);
