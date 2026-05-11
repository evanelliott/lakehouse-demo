# src/orchestration/lakehouse_dag.py
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
DEFAULT_ARGS = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- TASK LOGIC (Isolated for Portability) ---


def run_scraping_phase() -> None:
    """Phase 1: Bronze Ingestion via Hybrid Handshake."""
    from src.scraper.extraction import LakehouseScraper

    logger.info("🥉 Starting Bronze Scrape...")
    scraper = LakehouseScraper()

    # Understat
    understat_data = scraper.scrape_understat_data()
    for table_name, df in understat_data.items():
        scraper.upload_to_bronze(df, table_name)

    # Premier Injuries
    injury_df = scraper.scrape_injuries()
    scraper.upload_to_bronze(injury_df, "premier_injuries")


def run_transformation_phase() -> None:
    """Phase 2: Spark Silver Layer (3NF + SCD2)."""
    from pyspark.sql import SparkSession
    from src.modelling.transformations import LakehouseTransformer

    logger.info("🥈 Starting Silver Transformations...")
    spark = SparkSession.builder.getOrCreate()
    xfrm = LakehouseTransformer(spark)

    xfrm.init_silver_layer()
    xfrm.upsert_dim_teams()
    xfrm.upsert_dim_players_scd2()
    xfrm.write_fact_match_stats()


def run_quality_check_phase(execution_date: str) -> None:
    """Phase 3: DAMA DQ checks via DuckDB."""
    from src.analytics.quality import DataQualityEngine

    import duckdb

    logger.info(f"🧪 Running DQ Metrics for {execution_date}...")
    # Manual DuckDB setup for standalone/integration runs
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    # ... (S3 config from your snippet) ...

    engine = DataQualityEngine(con)
    engine.run(execution_date=execution_date)

    # Validation Gate
    query = "SELECT metric_name FROM silver.dq_metrics WHERE metric_value < 0.95"
    failed = con.execute(query).fetchall()
    if failed:
        raise ValueError(f"🛑 DQ Gate Failed: {failed}")


def run_gold_phase() -> None:
    """Phase 4: Gold Analytics Refresh."""
    from src.analytics.gold_layer import refresh_gold_views

    logger.info("🥇 Materializing Gold Layer...")
    # Logic to refresh views via DuckDB
    refresh_gold_views()


# --- STANDALONE RUNNER (For pre-push/integration hooks) ---
def execute_full_pipeline() -> None:
    """Standalone orchestrator that doesn't require Airflow."""
    today = datetime.now().strftime("%Y-%m-%d")
    run_scraping_phase()
    run_transformation_phase()
    run_quality_check_phase(execution_date=today)
    run_gold_phase()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    execute_full_pipeline()

# --- AIRFLOW DAG DEFINITION ---
try:
    from airflow import DAG
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from airflow.providers.standard.operators.python import PythonOperator

    with DAG(
        dag_id="pl_lakehouse_pipeline",
        default_args=DEFAULT_ARGS,
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["production", "lakehouse"],
    ) as dag:
        t1 = PythonOperator(task_id="scrape_bronze", python_callable=run_scraping_phase)

        t2 = PythonOperator(task_id="transform_silver", python_callable=run_transformation_phase)

        t3 = PythonOperator(
            task_id="run_dq_metrics",
            python_callable=run_quality_check_phase,
            op_kwargs={"execution_date": "{{ ds }}"},
        )

        t4 = SQLExecuteQueryOperator(
            task_id="refresh_gold_analytics",
            conn_id="duckdb_default",
            sql="analytics/gold_views.sql",
            split_statements=True,
        )

        t1 >> t2 >> t3 >> t4

except ImportError:
    # Allows the script to run in the 'test' profile without Airflow installed
    pass
