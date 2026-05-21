# tests/unit/warehouse/test_gold_queries.py
import os
import re
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import duckdb

SCHEMAS_DIR = "src/schemas"


def _load_query_from_view_file(file_name: str) -> str:
    """
    Utility to read a production view file and isolate its inner query logic.

    Uses a non-greedy structural regular expression to isolate and capture
    everything following the initial view signature declaration, completely
    protecting internal column aliases and common table expressions (CTEs).
    """
    file_path = os.path.join(SCHEMAS_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Target production schema file missing: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        # Normalise layout spacing by turning line breaks and tabs into singular spaces
        content = " ".join(f.read().split())

    # Structural Anchor Explanation:
    # (?i)                 -> Case-insensitive flag evaluation
    # ^CREATE\s+VIEW\s+    -> Enforces starting signature boundaries
    # (?:IF\s+NOT\s+EXISTS\s+)? -> Handles optional idempotency decorators safely
    # \w+\s+AS\s+          -> Matches the specific view name and its unique opening AS token
    # (.*)$                -> Captures the entire remaining query body as group 1
    pattern = r"(?i)^CREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?\w+\s+AS\s+(.*)$"
    match = re.match(pattern, content)

    if not match:
        raise ValueError(
            f"Malformed View Definition: Structural 'CREATE VIEW view_name AS' "
            f"signature could not be safely resolved inside {file_name}"
        )

    # Return the clean inner query body text string payload
    return match.group(1).strip().rstrip(";") + ";"


# =========================================================================
# ABSOLUTE FILE-DRIVEN VIEW VALIDATION GATES (U-GLD-01 to U-GLD-03)
# =========================================================================


@pytest.mark.idempotency
def test_gold_player_resilience_index_file_logic(
    duck_con: "duckdb.DuckDBPyConnection", seeded_metrics_data: None
) -> None:
    """U-GLD-01: Verifies production file query flattens injury dimensions cleanly."""
    production_query = _load_query_from_view_file("gold_player_resilience_index.sql")
    result = duck_con.execute(production_query).fetchone()

    assert result is not None
    # FIX: Unpack tuple elements via structured array index positions
    assert result[0] == 1164
    assert result[1] == "Adama Traore Diarra"
    assert result[2] == pytest.approx(0.966, rel=1e-3)


@pytest.mark.idempotency
def test_gold_squad_health_drag_file_logic(
    duck_con: "duckdb.DuckDBPyConnection", seeded_metrics_data: None
) -> None:
    """U-GLD-02: Verifies production file query isolates attacking drag variables."""
    production_query = _load_query_from_view_file("gold_squad_health_drag.sql")
    result = duck_con.execute(production_query).fetchone()

    assert result is not None
    assert result[0] == 29088
    assert result[1] == "West Ham United"
    assert result[2] == 1
    assert result[3] == pytest.approx(0.35, rel=1e-2)


@pytest.mark.idempotency
def test_gold_efficiency_anomalies_file_logic(
    duck_con: "duckdb.DuckDBPyConnection", seeded_metrics_data: None
) -> None:
    """U-GLD-03: Verifies production file query calculates points mismatch differentials."""
    production_query = _load_query_from_view_file("gold_efficiency_anomalies.sql")
    result = duck_con.execute(production_query).fetchone()

    assert result is not None
    assert result[0] == 29088
    assert result[1] == "West Ham United"
    assert result[2] == pytest.approx(-0.65, rel=1e-2)
    assert result[3] == pytest.approx(0.35, rel=1e-2)
