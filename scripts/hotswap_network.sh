#!/usr/bin/env bash
# scripts/hotswap_network.sh

CONTAINER_ID=$(hostname)

echo "🌐 Checking network state..."

# 1. Connect to air-gap if not already connected
if ! docker inspect "$CONTAINER_ID" --format '{{json .NetworkSettings.Networks}}' | grep -q "lakehouse-airgap"; then
    echo "🔗 Connecting to lakehouse-airgap..."
    docker network connect lakehouse-airgap "$CONTAINER_ID"
fi

# 2. Disconnect from bridge ONLY if it is still attached
if docker inspect "$CONTAINER_ID" --format '{{json .NetworkSettings.Networks}}' | grep -q "\"bridge\""; then
    echo "🔌 Disconnecting from bridge (internet)..."
    docker network disconnect bridge "$CONTAINER_ID"
else
    echo "✅ Already disconnected from bridge."
fi

echo "🔒 Container is sandboxed."
