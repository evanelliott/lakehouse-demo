-- Unpartitioned to support rapid broadcasting in multi-table historical joins
CREATE TABLE IF NOT EXISTS silver_dim_injury (
    player_id INTEGER NOT NULL,
    category VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL
)
USING iceberg;
