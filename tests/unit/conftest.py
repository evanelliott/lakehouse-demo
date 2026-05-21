# tests/unit/conftest.py
import os
from typing import TYPE_CHECKING, Generator

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.logging import LogCaptureFixture

    import duckdb

# --- ENGINE FIXTURES ---


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Dynamic Spark Session Fixture with strict hang guards.
    """
    is_integration = os.environ.get("INTEGRATION_TEST_MODE", "false").lower() == "true"
    spark_remote = os.environ.get("SPARK_REMOTE")

    if not is_integration:
        if not spark_remote:
            raise RuntimeError("CRITICAL: SPARK_REMOTE environment variable is missing!")

        # FIX: Appending the forward-slash safely allows ChannelBuilder to parse options
        remote_url = f"{spark_remote}/;timeout=5000;user_agent=pytest"
        print(f"📡 Connecting to Spark Connect daemon at: {remote_url}")

        session = SparkSession.builder.remote(remote_url).getOrCreate()

    else:
        # -----------------------------------------------------------------
        # INTEGRATION TEST WORKFLOW (Stateful Mock Infra Mode)
        # -----------------------------------------------------------------
        # Builds a local JVM executor capable of resolving Iceberg and S3A protocols.
        # JARs are loaded automatically because they are symlinked in the Dockerfile.
        sql_exts = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        lakehouse_uri = os.environ.get("ICEBERG_CATALOG_URI", "http://catalog-provider:8181")
        s3_endpoint = os.environ.get("S3_ENDPOINT", "http://storage-provider:9000")
        s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
        s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
        session = (
            SparkSession.builder.master("local[*]")
            .appName("lakehouse-integration-suite")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.extensions", sql_exts)
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", lakehouse_uri)
            .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse/")
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    yield session

    try:
        session.catalog.clearCache()
    except Exception:
        pass


@pytest.fixture(scope="function")
def duck_con() -> Generator["duckdb.DuckDBPyConnection", None, None]:
    """
    Provides an isolated, pristine in-memory DuckDB connection per test
    case to guarantee accurate evaluation of true idempotency loops.
    """
    import duckdb

    con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
    yield con
    con.close()


# --- LOGGING & OBSERVABILITY ---


@pytest.fixture(autouse=True)
def cap_logs(caplog: "LogCaptureFixture") -> "LogCaptureFixture":
    """
    Automatically captures logs for every test at the INFO level.
    Vital for verifying error messages in Negative Path tests.
    """
    caplog.set_level("INFO")
    return caplog


# --- CUSTOM MARKERS ---


def pytest_configure(config: "Config") -> None:
    """
    Registers custom markers to allow for targeted testing via prek.
    """
    # 3-Character Scraper & Utility Framework Markers
    config.addinivalue_line("markers", "utl: global core validation and sanitisation tests")
    config.addinivalue_line("markers", "net: network handshake and api connection tests")
    config.addinivalue_line("markers", "scr: data extraction and dictionary parsing tests")

    # Downstream Warehouse Progression Markers
    config.addinivalue_line("markers", "scd2: tests for temporal/history logic")
    config.addinivalue_line("markers", "idempotency: tests for re-run reliability")
    config.addinivalue_line("markers", "entity_resolution: tests for entity validation checks")
