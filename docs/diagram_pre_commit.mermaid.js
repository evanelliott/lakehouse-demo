graph TD
    %% Styling and Palettes
    classDef gate fill:#2d3748, stroke:#4a5568, stroke - width: 2px, color: #fff;
    classDef stage fill:#1a202c, stroke:#3182ce, stroke - width: 2px, color: #fff;
    classDef process fill:#2d3748, stroke:#718096, stroke - width: 1px, color: #e2e8f0;
    classDef testGroup fill:#234e52, stroke:#319795, stroke - width: 2px, color: #fff;
    classDef success fill:#1c4532, stroke:#38a169, stroke - width: 2px, color: #c6f6d5;

    %% Main Flow Structure
START([Git Commit Triggered])-- > LINT["1. Static Code Analysis<br><i>- Format & Lint (Ruff)<br>- Type Check (Mypy)</i>"]

LINT-- > PYTEST["2. Pytest Initialisation<br><i>- Config (pyproject.toml)</i>"]

PYTEST-- > HANDSHAKE["3. Spark Connect Handshake<br><i></i>"]

    subgraph SUITE["4. Run Unit Tests"]
        direction LR
G4["Scraper<br><b>10 Tests</b><br><i>Understat, Injuries</i>"]
G1["Entity Resolution<br><b>4 Tests</b><br><i>Players, Teams</i>"]
G3["SCD Type 2<br><b>6 Tests</b><br><i>Injury Timelines</i>"]
G2["Idempotency Gates<br><b>9 Tests</b><br><i>Spark Jobs, DuckDB</i>"]
end

HANDSHAKE-- > SUITE
SUITE-- > SUCCESS([✅ Unit Tests Passed! < br > Commit Finalised])

    %% Class Assignments
class LINT, PYTEST, HANDSHAKE process;
class RUN_SUITE gate;
class SUITE stage;
class G1, G2, G3, G4 testGroup;
class SUCCESS success;
