-- 1. TEAM HEALTH VS PERFORMANCE (The "Star Schema" Join)
-- Joins 3NF Facts with SCD2 Dimensions to see squad health impacts.
CREATE OR REPLACE VIEW gold_team_health_performance AS
WITH current_injuries AS (
    -- Get current injuries from SCD2 Dimension
    SELECT 
        team_id,
        COUNT(player_id) AS active_injuries
    FROM local.silver.dim_players
    WHERE is_current = true 
      AND injury_status IS NOT NULL
    GROUP BY team_id
)
SELECT 
    t.team_name,
    f.snapshot_date,
    SUM(f.xg) AS total_xg,
    SUM(f.actual_goals) AS total_goals,
    COALESCE(i.active_injuries, 0) AS injured_player_count,
    -- Analysis: Do more injuries lead to lower xG performance?
    ROUND(SUM(f.actual_goals) - SUM(f.xg), 2) AS finishing_efficiency
FROM local.silver.fact_match_stats f
JOIN local.silver.dim_teams t ON f.team_id = t.team_id
LEFT JOIN current_injuries i ON f.team_id = i.team_id
GROUP BY ALL;

-- 2. POINT-IN-TIME INJURY IMPACT (The "SCD2" History Join)
-- This join links a fact to the player's status AT THE TIME the fact occurred.
CREATE OR REPLACE VIEW gold_historical_player_impact AS
SELECT 
    p.player_name,
    p.team_name,
    f.snapshot_date,
    f.xg,
    p.injury_status,
    -- Check if the player was injured on the day of the snapshot
    CASE WHEN p.injury_status IS NOT NULL THEN 'Unavailable' ELSE 'Available' END as availability_status
FROM local.silver.fact_match_stats f
JOIN local.silver.dim_players p 
    ON f.player_id = p.player_id
    -- The "Magic" of SCD2: Match the fact date to the version of the dimension
    AND f.snapshot_date >= p.effective_from::DATE
    AND (f.snapshot_date < p.effective_to::DATE OR p.effective_to IS NULL);

-- 3. DATA QUALITY DASHBOARD (The "Governance" View)
-- Summarises the latest DAMA metrics for a quick health check.
CREATE OR REPLACE VIEW gold_data_governance_report AS
SELECT 
    table_name,
    dimension,
    metric_name,
    metric_value,
    CASE 
        WHEN unit = 'proportion' AND metric_value < 0.95 THEN '🔴 CRITICAL'
        WHEN unit = 'proportion' AND metric_value < 0.99 THEN '🟡 WARNING'
        ELSE '🟢 HEALTHY'
    END as status,
    check_timestamp
FROM local.silver.dq_metrics
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM local.silver.dq_metrics);
