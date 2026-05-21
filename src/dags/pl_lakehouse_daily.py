# src/dags/pl_lakehouse_daily.py
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

# Strict platform default parameters ensuring aggressive resource containment
DEFAULT_PLATFORM_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=1),
}

SCHEMAS_DIR = "/app/src/schemas"
TRANSFORMS_DIR = "/app/src/transforms"

# =========================================================================
# CLASSIC PYTHON OPERATOR EXECUTOR CALLABLES
# =========================================================================


def _identify_active_match_ids(**context: Any) -> List[int]:
    """Scans the Bronze dates dataset to locate matches played on run date."""
    logical_date_str: str = context["ds"]
    print(f"📆 Scanning historical match timelines for: {logical_date_str}")
    mock_discovered_matches: List[int] = []
    return mock_discovered_matches


def _scrape_understat_match_instance(match_id: int, **context: Any) -> Dict[str, Any]:
    """Processes a single target match execution context via mapped loops."""
    print(f"📡 Executing outbound handshake network client loop for Match ID: {match_id}")
    return {"match_id": match_id, "status": "extracted"}


def _load_query_script(file_name: str) -> str:
    """Helper to parse external DDL/DML script files from the schemas folder."""
    file_path = os.path.join(SCHEMAS_DIR, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read().strip()


# =========================================================================
# CORE WORKFLOW GRAPH DEFINITIONS
# =========================================================================


with DAG(
    dag_id="pl_lakehouse_daily",
    default_args=DEFAULT_PLATFORM_ARGS,
    description="Atomic E2E Lakehouse Pipeline: Dynamic Ingestion -> Silver -> Gold",
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    # -------------------------------------------------------------------------
    # TIER 1: DATA ACQUISITION & INGESTION LAYER (BRONZE)
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="bronze", tooltip="Data Lake Acquisition Tasks") as bronze_group:
        scrape_premier_injuries = EmptyOperator(
            task_id="scrape_premier_injuries_data",
            execution_timeout=timedelta(minutes=1),
        )

        scrape_understat_league = EmptyOperator(
            task_id="scrape_understat_league_data",
            execution_timeout=timedelta(minutes=1),
        )

        identify_active_match_ids = PythonOperator(
            task_id="identify_active_match_ids",
            python_callable=_identify_active_match_ids,
            execution_timeout=timedelta(minutes=1),
        )

        scrape_understat_match_instance = PythonOperator.partial(
            task_id="scrape_understat_match_instance",
            python_callable=_scrape_understat_match_instance,
            execution_timeout=timedelta(minutes=1),
        ).expand(op_kwargs=identify_active_match_ids.output.map(lambda x: {"match_id": x}))

        scrape_understat_league >> identify_active_match_ids >> scrape_understat_match_instance

    # -------------------------------------------------------------------------
    # TIER 2: WAREHOUSE DIMENSIONAL MODEL LAYER (SILVER)
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="silver", tooltip="Warehouse Core Dimensional Logic") as silver_group:
        # Job 1: Executes ER mapping and inserts transactional SCD2 timelines
        silver_modelling = SparkSubmitOperator(
            task_id="silver_modelling",
            conn_id="spark_default",
            application=os.path.join(TRANSFORMS_DIR, "normalisation.py"),
            name="silver_modelling_normalization_job",
            application_args=["--processing-date", "{{ ds }}"],
            conf={"spark.sql.iceberg.handle-timestamp-without-timezone": "true"},
            execution_timeout=timedelta(minutes=1),
        )

        # Job 2: Runs career-to-date lookbacks and aggregates squad health deficits
        silver_analytics = SparkSubmitOperator(
            task_id="silver_analytics",
            conn_id="spark_default",
            application=os.path.join(TRANSFORMS_DIR, "metrics.py"),
            name="silver_analytics_metrics_job",
            application_args=["--processing-date", "{{ ds }}"],
            conf={"spark.sql.shuffle.partitions": "10"},
            execution_timeout=timedelta(minutes=1),
        )

        silver_modelling >> silver_analytics

    # -------------------------------------------------------------------------
    # TIER 3: PERFORMANCE ANALYTICS SERVING VIEWS ASSEMBLY (GOLD)
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="gold", tooltip="Analytical Views & Aggregations") as gold_group:
        refresh_player_resilience = SQLExecuteQueryOperator(
            task_id="refresh_gold_player_resilience_index",
            conn_id="duckdb",
            sql=_load_query_script("gold_player_resilience_index.sql"),
            execution_timeout=timedelta(minutes=1),
        )

        refresh_squad_health = SQLExecuteQueryOperator(
            task_id="refresh_gold_squad_health_drag",
            conn_id="duckdb",
            sql=_load_query_script("gold_squad_health_drag.sql"),
            execution_timeout=timedelta(minutes=1),
        )

        refresh_efficiency_anomalies = SQLExecuteQueryOperator(
            task_id="refresh_gold_efficiency_anomalies",
            conn_id="duckdb",
            sql=_load_query_script("gold_efficiency_anomalies.sql"),
            execution_timeout=timedelta(minutes=1),
        )

        [refresh_player_resilience, refresh_squad_health, refresh_efficiency_anomalies]

    # =========================================================================
    # DEPENDENCY TOPOLOGY MAP
    # =========================================================================
    bronze_group >> silver_group >> gold_group
