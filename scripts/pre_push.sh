#!/bin/bash
# scripts/pre_push.sh
echo "🏗️  Starting Integration Plumbing Check..."

# This specifically starts MinIO, Catalog, and the Tester
docker compose --profile test up --exit-code-from integration-tester

# Cleanup
docker compose --profile test down
