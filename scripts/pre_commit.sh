#!/bin/bash
set -e

echo "🔍 Running Ruff (Lint & Fix)..."
ruff check --fix .

echo "🎨 Running Ruff (Format)..."
ruff format .

echo "🏗️  Running Mypy (Type Check)..."
mypy .

echo "🧪 Running Unit Tests..."
# We use -m "not integration" to ensure we stay air-gapped
pytest tests/unit/ -m "not integration"

echo "✅ All checks passed!"
