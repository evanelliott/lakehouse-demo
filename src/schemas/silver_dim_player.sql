CREATE TABLE IF NOT EXISTS silver_dim_player (
    id INTEGER PRIMARY KEY,
    player_name VARCHAR NOT NULL
)
USING iceberg;
