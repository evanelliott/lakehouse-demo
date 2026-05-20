# tests/unit/conftest.py
import os
import time
from typing import TYPE_CHECKING, Generator

import pytest
from py4j.protocol import Py4JNetworkError
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.logging import LogCaptureFixture

    import duckdb

# --- ENGINE FIXTURES ---


# tests/unit/conftest.py


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provides a stable, high-memory embedded local Spark session for unit tests.

    Handles rapid socket reclamation loops and resilient process teardowns cleanly.
    """
    if "JAVA_HOME" in os.environ:
        del os.environ["JAVA_HOME"]

    # Allocate 4GB heap space pool natively before boot
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 4g pyspark-shell"

    for attempt in range(3):
        try:
            session = (
                SparkSession.builder.master("local[*]")
                .appName("StatelessUnitTesting")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.default.parallelism", "1")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.log.level", "WARN")
                .getOrCreate()
            )
            break
        except (ConnectionRefusedError, Py4JNetworkError):
            if attempt == 2:
                raise
            time.sleep(0.5)

    yield session

    # FIX: Safety wrap the teardown phase. If an upstream negative validation test
    # forced the local JVM background process to terminate or panic early,
    # we catch the socket drop cleanly to prevent teardown exit code crashes.
    try:
        session.stop()
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
    config.addinivalue_line("markers", "scraper: tests for extraction logic")
    config.addinivalue_line("markers", "scd2: tests for temporal/history logic")
    config.addinivalue_line("markers", "idempotency: tests for re-run reliability")
    config.addinivalue_line("markers", "entity_resolution: tests for entity validation checks")
