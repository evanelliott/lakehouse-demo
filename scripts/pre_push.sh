#!/usr/bin/env bash
# scripts/pre_push.sh
set -e

echo "🏗️  Executing Infrastructure Integration Gate (pre-push)..."

# 1. Warm-start the test profile mesh. 
# --build quickly evaluates code edits; --exit-code-from returns the pytest results.
docker compose --profile test up --build --exit-code-from integration-tester

echo "⏸️  Pausing test stack..."
# 2. Stop the containers instantly without the overhead of tearing down virtual networks.
docker compose --profile test stop

echo "✅ Integration gate completed successfully!"
