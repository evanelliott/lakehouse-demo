CREATE VIEW IF NOT EXISTS gold_squad_health_drag AS
SELECT 
    m.match_id,
    t.title AS home_team_title,
    d.active_injured_count,
    d.total_squad_xg_drag
FROM silver_fact_match m
INNER JOIN silver_dim_team t 
    ON m.team_h = t.id
INNER JOIN silver_analytics_match_squad_health_deficit d 
    ON m.match_id = d.match_id AND m.team_h = d.team_id;
