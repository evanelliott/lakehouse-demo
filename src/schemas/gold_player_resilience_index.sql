CREATE VIEW IF NOT EXISTS gold_player_resilience_index AS
SELECT 
    p.id AS player_id,
    p.player_name,
    i.availability_coefficient
FROM silver_dim_player p
INNER JOIN silver_analytics_player_injury_summary i 
    ON p.id = i.player_id
WHERE i.is_current = true;
