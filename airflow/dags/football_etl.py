from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

# Configuration - these match your .env and connection setup
TRINO_CONN_ID = "trino_conn"
CATALOG = "iceberg"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

default_args = {
    "owner": "senior_de",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "football_lakehouse_etl",
    default_args=default_args,
    description="Refine raw football match events into a partitioned Silver table",
    schedule_interval="@daily",
    catchup=False,
    tags=["lakehouse", "iceberg", "football"],
) as dag:

    # 1. Infrastructure Setup: Ensure Silver Schema exists
    create_silver_schema = TrinoOperator(
        task_id="create_silver_schema",
        trino_conn_id=TRINO_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}",
    )

    # 2. Schema Evolution: Ensure Silver Table exists with partitioning
    # Senior Tip: Partitioning by 'match_date' optimizes query performance
    create_silver_table = TrinoOperator(
        task_id="create_silver_table",
        trino_conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}.match_events (
                event_id VARCHAR,
                match_id VARCHAR,
                team_name VARCHAR,
                player_name VARCHAR,
                event_type VARCHAR,
                match_minute INTEGER,
                event_timestamp TIMESTAMP(6) WITH TIME ZONE,
                match_date DATE
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['match_date']
            )
        """,
    )

    # 3. The "Senior" Move: Idempotent MERGE (Upsert)
    # This prevents duplicates if the DAG is re-run and cleans the data
    refine_events = TrinoOperator(
        task_id="refine_bronze_to_silver",
        trino_conn_id=TRINO_CONN_ID,
        sql=f"""
            MERGE INTO {CATALOG}.{SILVER_SCHEMA}.match_events AS target
            USING (
                SELECT 
                    event_id,
                    match_id,
                    UPPER(team_name) as team_name, -- Data Standardization
                    player_name,
                    event_type,
                    CAST(match_minute AS INTEGER) as match_minute,
                    CAST(event_time AS TIMESTAMP(6) WITH TIME ZONE) as event_timestamp,
                    CAST(event_time AS DATE) as match_date
                FROM {CATALOG}.{BRONZE_SCHEMA}.raw_events
                WHERE event_type IN ('GOAL', 'CARD', 'SUBSTITUTION') -- Data Filtering
            ) AS source
            ON (target.event_id = source.event_id)
            WHEN NOT MATCHED THEN
                INSERT (event_id, match_id, team_name, player_name, event_type, match_minute, event_timestamp, match_date)
                VALUES (source.event_id, source.match_id, source.team_name, source.player_name, source.event_type, source.match_minute, source.event_timestamp, source.match_date)
        """,
    )

    create_silver_schema >> create_silver_table >> refine_events
