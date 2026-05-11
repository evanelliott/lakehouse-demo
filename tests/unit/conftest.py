import json
import os
from typing import TYPE_CHECKING, Callable, Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.logging import LogCaptureFixture

    import duckdb

# --- ENGINE FIXTURES ---


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Connects to the background Spark Connect server.
    Ensures zero JVM startup latency for unit tests.
    """
    session = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def duck_con() -> Generator["duckdb.DuckDBPyConnection", None, None]:
    """
    Provides a shared, in-memory DuckDB connection for analytical unit tests.
    """
    import duckdb

    con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
    yield con
    con.close()


# --- SCHEMA & CONTRACT FIXTURES ---


@pytest.fixture(scope="session")
def get_schema() -> Callable[[str], StructType]:
    """
    Provides a helper function to load Spark StructTypes from your central
    schemas directory. This ensures tests are always in sync with your contracts.
    """

    def _load(schema_name: str) -> StructType:
        schema_path = f"/app/schemas/{schema_name}.json"
        if not os.path.exists(schema_path):
            pytest.fail(f"Schema file not found: {schema_path}")

        with open(schema_path, "r") as f:
            return StructType.fromJson(json.load(f))

    return _load


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
    config.addinivalue_line("markers", "standardisation: tests for entity resolution")
