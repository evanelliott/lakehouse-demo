#!/usr/bin/env bash
# scripts/pre_push.sh
set -e

echo "========================================================================"
echo "🏗️  Executing Infrastructure Integration Gate (pre-push)..."
echo "========================================================================"

# 1. Warm-start the test profile mesh with the explicit mode toggle.
# --build evaluates code edits; --exit-code-from returns the pytest results.
INTEGRATION_TEST_MODE=true docker compose --profile test up --build --exit-code-from integration-tester

echo "⏸️  Pausing test stack..."
# 2. Stop the containers instantly without the overhead of tearing down virtual networks.
docker compose --profile test stop

echo "========================================================================"
echo "✅ Integration gate completed successfully!"
echo "========================================================================"
