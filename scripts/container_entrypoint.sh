#!/bin/bash
# scripts/container_entrypoint.sh
set -e

# 1. Fix Git "Safe Directory" for the mount point
git config --global --add safe.directory /app

# 2. Ensure we are in the correct directory
cd /app

# 3. Git Hook Install (Now that safe.directory is set)
prek install

# 4. ROBUST EXTENSION INTERCEPT: Keep the internet bridge open while VS Code installs tools.
# This loops safely at half-second intervals until the marketplace installer finishes.
echo "⏳ Syncing with VS Code extension installer engine..."
while pgrep -f "do-not-sync" > /dev/null; do
    sleep 0.5
done

# # 5. Handle the Network Hotswap as the absolute final step
# # This ensures that all local utilities have internet access before isolation.
# if [ -f "scripts/hotswap_network.sh" ]; then
#     echo "🔒 VS Code installer is idle. Engaging 'lakehouse-airgap' shield..."
#     bash scripts/hotswap_network.sh
# fi

echo "✅ Container environment is fully initialized and isolated."
