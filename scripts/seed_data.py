import trino
import os
from dotenv import load_dotenv

# Load environment variables from the root .env file
load_dotenv()

# --- CONFIGURATION (Mapped from .env) ---
TRINO_HOST = "localhost"
TRINO_PORT = os.getenv("TRINO_PORT", 8080)
TRINO_USER = "admin"
CATALOG = "iceberg"
BRONZE_SCHEMA = "bronze"

def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user=TRINO_USER,
        catalog=CATALOG,
    )

def main():
    print(f"🏗️  Seeding Lakehouse metadata at {TRINO_HOST}:{TRINO_PORT}...")
    
    try:
        conn = get_connection()
        cur = conn.cursor()

        # 1. Create the Bronze Schema (Namespace)
        print(f"Creating schema: {CATALOG}.{BRONZE_SCHEMA}...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

        # 2. Create the Raw Events Table (Iceberg)
        # Note: We use 'event_time' for partitioning to show high-performance design
        print(f"Creating table: {CATALOG}.{BRONZE_SCHEMA}.raw_events...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_events (
                event_id VARCHAR,
                match_id VARCHAR,
                team_name VARCHAR,
                player_name VARCHAR,
                event_type VARCHAR,
                match_minute INTEGER,
                event_time TIMESTAMP(6) WITH TIME ZONE
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(event_time)'],
                location = 's3://warehouse/bronze/raw_events'
            )
        """)

        print("✅ Seed successful: Bronze layer is ready for ingestion.")

    except Exception as e:
        print(f"❌ Seed failed: {e}")
        print("Tip: Make sure 'make up' has finished and Trino is healthy.")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
