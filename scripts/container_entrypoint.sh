#!/bin/bash
set -e

# 1. Fix Git "Safe Directory" for the mount point
git config --global --add safe.directory /app

# 2. Ensure we are in the correct directory
cd /app

# 3. Handle the Network Hotswap
# (Connecting to airgap, disconnecting from bridge)
if [ -f "scripts/hotswap_network.sh" ]; then
    bash scripts/hotswap_network.sh
fi

# 4. Environment Setup (The stuff that needs internet or local tools)
# If this script needs internet, run it BEFORE the hotswap or 
# ensure the hotswap only happens after this succeeds.
bash scripts/dev_setup.sh

# 5. Git Hook Install (Now that safe.directory is set)
prek install

echo "✅ Container environment is fully initialized."
