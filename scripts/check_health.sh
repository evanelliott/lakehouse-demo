#!/bin/bash

# --- Load Environment Variables ---
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ Error: .env file not found."
    exit 1
fi

# --- The "Senior" Health Check Function ---
# Args: $1=Service Name, $2=URL, $3=Valid Statuses (space separated)
check_health() {
    local name=$1
    local url=$2
    local valid_codes=$3
    
    # Padding for clean alignment
    local padded_name=$(printf "%-20s" "$name")

    # Fetch status code
    local status=$(curl -s -o /dev/null -w "%{http_code}" "$url")

    # Check if the returned status is in our valid_codes list
    if [[ " $valid_codes " =~ " $status " ]]; then
        echo "Checking $padded_name ✅ OK (Status: $status)"
        return 0
    elif [ "$status" == "000" ]; then
        echo "Checking $padded_name ⏳ STARTING (Status: 000)"
        return 1
    else
        echo "Checking $padded_name ❌ FAILED (Status: $status)"
        return 1
    fi
}

echo "========================================="
echo "📊 LAKEHOUSE STACK HEALTH CHECK"
echo "========================================="

# Run checks with specific valid status expectations
check_health "MinIO API"       "http://localhost:${MINIO_API_PORT}/minio/health/live" "200"
check_health "Iceberg Catalog" "http://localhost:${CATALOG_PORT}/v1/config"           "200"
check_health "Trino Engine"    "http://localhost:${TRINO_PORT}/ui/"                  "200 302 303"
check_health "Airflow UI"      "http://localhost:${AIRFLOW_PORT}/health"             "200"
check_health "Jupyter Lab"     "http://localhost:${JUPYTER_PORT}/api"                "200"

echo "========================================="

# Final logic to guide the user
echo "🚀 If Trino is 303 and Airflow is 200, proceed to 'make seed'."
echo "-----------------------------------------"
