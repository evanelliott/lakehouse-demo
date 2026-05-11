#!/bin/bash
set -e

# --- 1. WAIT FOR INFRASTRUCTURE ---
# Ensure Postgres is actually accepting connections before we proceed
echo "Checking database connectivity..."
until airflow db check; do
  echo "Postgres is unavailable - sleeping"
  sleep 2
done

# --- 2. IDEMPOTENT DATABASE SETUP ---
# check-migrations returns a non-zero exit code if the DB is empty or needs updates
if ! airflow db check-migrations > /dev/null 2>&1; then
    echo "Database is empty or requires migrations. Initializing..."
    airflow db init
else
    echo "Database is already initialized. Skipping init."
fi

# --- 3. IDEMPOTENT USER CREATION ---
# We use a simple hidden file to track if we've already created the admin
ADMIN_LOCK="/app/logs/.admin_created"

if [ ! -f "$ADMIN_LOCK" ]; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Data \
        --lastname Engineer \
        --role Admin \
        --email engineer@example.com && touch "$ADMIN_LOCK"
else
    echo "Admin user already exists. Skipping user creation."
fi

# --- 4. START ORCHESTRATION ---
# Start the scheduler in the background
echo "Starting Airflow Scheduler..."
airflow scheduler &

# Start the webserver in the foreground (this keeps the container alive)
echo "Starting Airflow Webserver..."
exec airflow webserver
