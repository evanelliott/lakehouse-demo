#!/bin/bash
set -e

# --- CONFIGURATION ---
# These match the networks defined in your docker-compose.yml
AIRGAP_NET="lakehouse-airgap"
LIVE_NET="lakehouse-live"

echo "----------------------------------------------------------"
echo "🛠️  Initializing Strategic Network Infrastructure..."
echo "----------------------------------------------------------"

# 1. Create the Internal (Air-Gapped) Network
# This network has NO gateway to the internet. 
# It enforces your 'Stateless' Pre-commit strategy.
if ! docker network inspect "$AIRGAP_NET" >/dev/null 2>&1; then
    echo "🌐 Creating internal-only network: $AIRGAP_NET"
    docker network create --internal "$AIRGAP_NET"
else
    echo "✅ Internal network '$AIRGAP_NET' already exists."
fi

# 2. Create the Live (Bridge) Network
# This allows the Airflow container to reach external sites 
# for Periodic Smoke Tests / Scrapers.
if ! docker network inspect "$LIVE_NET" >/dev/null 2>&1; then
    echo "🌐 Creating live-access network: $LIVE_NET"
    docker network create --driver bridge "$LIVE_NET"
else
    echo "✅ Live network '$LIVE_NET' already exists."
fi

# 3. Docker Socket Connectivity Check
# We check if the socket exists. On Mac, we don't need sudo chmod 
# because Docker Desktop handles the bridge to the VM for us.
if [ -S /var/run/docker.sock ]; then
    echo "🔌 Docker socket detected at /var/run/docker.sock"
else
    echo "⚠️  WARNING: Docker socket not found. Ensure Docker Desktop is running."
fi

echo "----------------------------------------------------------"
echo "🚀 Environment is now sandboxed and ready for devcontainer."
echo "----------------------------------------------------------"
