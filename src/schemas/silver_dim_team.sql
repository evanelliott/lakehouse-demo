CREATE TABLE IF NOT EXISTS silver_dim_team (
    id INTEGER PRIMARY KEY,
    title VARCHAR NOT NULL
)
USING iceberg;
