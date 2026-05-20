#!/usr/bin/env bash
# scripts/dev_setup.sh
set -e 

echo "🛠️ Starting DevContainer setup..."

# Trust the git directory context inside the container path
git config --global --add safe.directory /app

# Activate the python virtual environment
source /opt/venv/bin/activate

echo "✅ DevContainer setup complete!"
