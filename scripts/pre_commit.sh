#!/usr/bin/env bash
# scripts/pre_commit.sh
set -e

echo "========================================================================"
echo "🔍 Initiating Fast Unit Gate (pre-commit)..."
echo "========================================================================"

# 1. Clean code formatting gates
ruff check --fix .
ruff format .

# Force Mypy to compile its type maps inside an isolated, temporary folder.
# This prevents parallel VS Code dry-run threads from hitting lock collisions.
mypy --show-traceback --cache-dir=/tmp/mypy_cache_commit .

# Safely track and stage Ruff's auto-fixes inside the active commit transaction
MODIFIED_FILES=$(git diff --cached --name-only --diff-filter=ACM)
if [ -n "$MODIFIED_FILES" ]; then
    echo "✨ Ruff modified code formatting. Updating Git index safely..."
    echo "$MODIFIED_FILES" | xargs -I {} git add "{}" < /dev/null
fi

# 2. Trigger stateless local test suite
echo "▶ Launching unit test collection..."
# Pytest automatically sniffs SPARK_REMOTE out from pyproject.toml to run via Spark Connect
PYTHONWARNINGS="ignore" pytest --record-mode=none < /dev/null

echo "========================================================================"
echo "✅ Fast Unit Gate Cleared! Commit processing finalized."
echo "========================================================================"
