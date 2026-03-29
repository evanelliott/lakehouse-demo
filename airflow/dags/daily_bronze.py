import json
import os
import re
import requests
import codecs
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- CONFIGURATION ---
# Premier League 23/24 started on 2023-08-11
START_DATE = datetime(2026, 3, 11)
BASE_URL = "https://understat.com"
# Internal path inside the Docker container mapped to your local ./data/minio
RAW_STORAGE_PATH = "/opt/airflow/data/minio/warehouse/bronze/raw_matches"

def scrape_match_data(ds, **kwargs):
    """
    ds: Airflow execution date (YYYY-MM-DD).
    Resilient to Understat's hex-encoded strings and variable name shifts.
    """
    print(f"🔍 Searching for matches on: {ds}")
    
    # 1. Fetch the League Page with a professional User-Agent
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'}
    response = requests.get(BASE_URL, headers=headers, timeout=15)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    scripts = soup.find_all('script')
    
    raw_blob = None
    # 2. Extract the JSON blob using Raw String Regex
    # We look for the content inside JSON.parse('...') or JSON.parse("...")
    for s in scripts:
        content = s.get_text()
        if 'JSON.parse' in content:
            # r"..." tells Python NOT to interpret backslashes in the regex itself
            match = re.search(r"JSON\.parse\s*\(['\"](.*?)['\"]\)", content)
            if match:
                raw_blob = match.group(1)
                break

    if not raw_blob:
        raise ValueError(f"CRITICAL: No data blob found in HTML for {ds}. First 200 chars: {response.text[:200]}")

    # 3. THE SENIOR DECODE: Convert hex (\xHH) to valid JSON characters
    try:
        # We decode the raw escape sequences into a clean UTF-8 string
        decoded_string = codecs.decode(raw_blob, 'unicode_escape')
        print(f"decoded_string snippet: {repr(decoded_string[:10000])}")  # Show a snippet of the decoded string for debugging
        data = json.loads(decoded_string)
        print(f"data snippet: {json.dumps(data)[:10000]}")
        
        # Normalize: Handle single object (match_info) vs list (datesData)
        if isinstance(data, dict):
            data = [data]
            
    except Exception as e:
        print(f"❌ Decode/JSON Error: {e}")
        print(f"Snippet of problematic blob: {repr(raw_blob[:100])}")
        raise

    # 4. Filter for Match IDs on this specific logical date ('ds')
    # Handles both 'date' and 'datetime' keys for future-proofing
    match_ids = [
        m['id'] for m in data 
        if ds in str(m.get('date', m.get('datetime', '')))
    ]

    if not match_ids:
        print(f"⚽ No matches scheduled for {ds}. Task complete.")
        return f"NO_MATCHES_ON_{ds}"

    # 5. Scrape and Save Shot Data for each Match
    for m_id in match_ids:
        print(f"📦 Scraping Match ID: {m_id}")
        m_res = requests.get(f"https://understat.com{m_id}", headers=headers, timeout=15)
        m_res.raise_for_status()
        
        m_soup = BeautifulSoup(m_res.content, 'html.parser')
        m_scripts = m_soup.find_all('script')
        
        shots_json = None
        for ms in m_scripts:
            m_content = ms.get_text()
            if 'var shotsData' in m_content:
                # Same robust regex pattern for individual match pages
                m_match = re.search(r"var shotsData\s*=\s*JSON\.parse\s*\(['\"](.*?)['\"]\)", m_content)
                if m_match:
                    shots_json = codecs.decode(m_match.group(1), 'unicode_escape')
                    break
        
        if shots_json:
            # Save to the mounted MinIO/S3 volume
            os.makedirs(RAW_STORAGE_PATH, exist_ok=True)
            file_path = f"{RAW_STORAGE_PATH}/match_{m_id}_{ds}.json"
            with open(file_path, "w") as f:
                f.write(shots_json)
            print(f"✅ Saved Bronze JSON: {file_path}")

    # Final return for XCom visibility
    return decoded_string

# --- DAG DEFINITION ---
default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': START_DATE,
    # --- NATIVE RESILIENCE FEATURES ---
    'retries': 0,                           # retry up to 5 times
    'retry_delay': timedelta(minutes=5),    # wait 5 mins between tries
    'retry_exponential_backoff': True,      # WAIT LONGER each time (5, 10, 20... mins)
    'max_retry_delay': timedelta(hours=1),  # cap the wait at 1 hour
}

with DAG(
    'daily-bronze',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=True,
    # --- RATE LIMITING ---
    max_active_runs=3,      # Only run 3 days at a time (try to avoid server-side rate limits)
    tags=['native', 'bronze'],
) as dag:

    scrape = PythonOperator(
        task_id='scrape',
        python_callable=scrape_match_data,
        # execution_timeout prevents a task from running forever
        execution_timeout=timedelta(minutes=10) 
    )
