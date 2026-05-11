#!/usr/bin/env bash
# scripts/dev_setup.sh
set -e 

echo "🛠️ Starting DevContainer setup..."

# 1. Trust the git directory
git config --global --add safe.directory /app

# 2. Ensure all scripts are executable
chmod +x scripts/*.sh

# 3. Skip Sync (Already handled by Dockerfile system install)
echo "📦 Environment already provisioned via Dockerfile. Skipping sync."

# 5. Start Spark Connect server
echo "⚡ Starting Spark Connect Server (Background)..."
/usr/local/bin/spark-shell --version # Warm up the JVM
/usr/local/bin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.0 &

echo "✅ DevContainer setup complete!"
