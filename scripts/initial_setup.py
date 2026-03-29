import requests
import json
import re
import os
from bs4 import BeautifulSoup
from trino.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
URL = "https://understat.com/league/EPL"
TRINO_CONFIG = {
    "host": "localhost",
    "port": int(os.getenv("TRINO_PORT", 8080)),
    "user": "admin",
    "catalog": "iceberg",
    "schema": "silver"
}

def get_understat_json(url, var_name):
    """Extracts and decodes JSON from Understat script tags."""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    scripts = soup.find_all('script')
    for s in scripts:
        if var_name in str(s):
            pattern = re.compile(rf"var {var_name} = JSON.parse\('(.*?)'\);")
            match = pattern.search(s.string)
            if match:
                data = match.group(1).encode('utf-8').decode('unicode_escape')
                return json.loads(data)
    return None

def setup_lakehouse():
    conn = connect(**TRINO_CONFIG)
    cur = conn.cursor()

    # --- 1. TEAMS DATA ---
    print("⏳ Processing Team Data...")
    teams_raw = get_understat_json(URL, 'teamsData')
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.epl_teams (
            team_id VARCHAR, team_name VARCHAR, xG DOUBLE, xGA DOUBLE, pts INTEGER, scraped_at TIMESTAMP(6) WITH TIME ZONE
        )
    """)
    
    team_rows = []
    for tid, info in teams_raw.items():
        hist = info['history'][-1] # Get latest season aggregates
        team_rows.append((tid, info['title'], hist['xG'], hist['xGA'], hist['pts']))
    
    cur.executemany("INSERT INTO iceberg.silver.epl_teams VALUES (?, ?, ?, ?, ?, current_timestamp)", team_rows)

    # --- 2. PLAYERS DATA ---
    print("⏳ Processing Player Data...")
    players_raw = get_understat_json(URL, 'playersData')
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.epl_players (
            player_id VARCHAR, player_name VARCHAR, team_name VARCHAR, 
            games INTEGER, goals INTEGER, assists INTEGER, 
            xG DOUBLE, xA DOUBLE, scraped_at TIMESTAMP(6) WITH TIME ZONE
        )
    """)
    
    player_rows = []
    for p in players_raw:
        player_rows.append((
            p['id'], p['player_name'], p['team_title'],
            int(p['games']), int(p['goals']), int(p['assists']),
            float(p['xG']), float(p['xA'])
        ))
    
    cur.executemany("INSERT INTO iceberg.silver.epl_players VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)", player_rows)

    print(f"✅ Setup Complete. Ingested {len(team_rows)} teams and {len(player_rows)} players.")
    conn.close()

if __name__ == "__main__":
    setup_lakehouse()
