import os

import duckdb


def refresh_gold_views(con: duckdb.DuckDBPyConnection) -> None:
    """Executes the Gold View definitions against the Silver tables."""
    print("🏆 Materialising Gold Analytics Layer...")

    # Path to the SQL file we built earlier
    sql_path = os.path.join(os.path.dirname(__file__), "gold_views.sql")

    with open(sql_path, "r") as f:
        sql_commands = f.read()

    # DuckDB handles multiple CREATE VIEW statements in one execute call
    con.execute(sql_commands)
    print("✅ Gold Views Refreshed.")
