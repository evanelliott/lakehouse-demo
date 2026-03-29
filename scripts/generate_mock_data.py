import trino
import uuid
import random
from datetime import datetime, timezone

# --- CONFIGURATION (Matches your .env/internal network) ---
# Note: When running from your Mac host, use 'localhost' and your TRINO_PORT
TRINO_HOST = 'localhost'
TRINO_PORT = 8080
TRINO_USER = 'admin'
CATALOG = 'iceberg'
SCHEMA = 'bronze'

def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=CATALOG,
        schema=SCHEMA,
    )

def generate_event(match_id, teams):
    event_types = ['GOAL', 'CARD', 'SUBSTITUTION', 'FOUL', 'VAR_CHECK']
    team = random.choice(teams)
    player = f"Player_{random.randint(1, 22)}"
    
    return (
        str(uuid.uuid4()),        # event_id
        match_id,                 # match_id
        team,                     # team_name
        player,                   # player_name
        random.choice(event_types),# event_type
        random.randint(1, 95),    # match_minute
        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f') # event_time
    )

def main():
    print(f"🚀 Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    conn = get_connection()
    cur = conn.cursor()

    # Define the match context
    match_id = str(uuid.uuid4())
    teams = ['Arsenal', 'Manchester City']
    num_events = 20

    print(f"⚽ Generating {num_events} events for {teams[0]} vs {teams[1]}...")
    
    events = [generate_event(match_id, teams) for _ in range(num_events)]

    # Batch Insert into Bronze (Raw) table
    # We use a parameterized query for security/best practice
    sql = f"""
        INSERT INTO {CATALOG}.{SCHEMA}.raw_events 
        (event_id, match_id, team_name, player_name, event_type, match_minute, event_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cur.executemany(sql, events)
        print(f"✅ Successfully ingested {num_events} events into {CATALOG}.{SCHEMA}.raw_events")
    except Exception as e:
        print(f"❌ Error during ingestion: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
