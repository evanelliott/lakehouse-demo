#!/usr/bin/env bash
# scripts/generate_cassettes.sh
set -e

CONTAINER_NAME="lakehouse-dev-container"
TARGET_TEST="tests/unit/scraper/test_scraper_network_handshake.py"

echo "========================================================================"
echo "🔒 Current State: Engaging Temporary Internet Bridge Connection..."
echo "========================================================================"

# 1. Inject the default internet-enabled bridge network into your container card
if ! docker network connect bridge "$CONTAINER_NAME" 2>/dev/null; then
    echo "ℹ️  Container is already connected to the bridge network or interface exists."
else
    echo "✅ Internet network card successfully attached to $CONTAINER_NAME."
fi

# 2. Add a sub-second trap handle rule to guarantee the bridge is disconnected
# even if an unexpected keyboard interrupt (Ctrl+C) or pytest failure occurs.
cleanup() {
    echo ""
    echo "========================================================================"
    echo "🔒 Re-engaging Shield: Severing Internet Bridge Interface..."
    echo "========================================================================"
    docker network disconnect bridge "$CONTAINER_NAME" 2>/dev/null || true
    echo "✅ Total air-gapped sandbox isolation successfully re-established."
    echo "========================================================================"
}
trap cleanup EXIT

echo "▶ Launching live web recording transaction loop inside container..."
echo "------------------------------------------------------------------------"

# 3. Trigger the recording run using your container python environment mappings
docker exec -it "$CONTAINER_NAME" pytest "$TARGET_TEST" -m net --record-mode=once

echo "------------------------------------------------------------------------"
echo "✨ Network responses successfully captured and written to cassettes folder."
