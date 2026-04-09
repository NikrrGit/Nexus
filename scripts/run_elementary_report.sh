#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../dbt"

echo "==> Installing dbt packages (including Elementary)..."
dbt deps

echo "==> Building Elementary internal tables..."
dbt run --select elementary

echo "==> Running all dbt tests..."
dbt test

echo "==> Generating Elementary observability report..."
mkdir -p ../edr_target
edr report --file-path ../edr_target/report.html --project-dir .

echo "==> Done. Report saved to edr_target/report.html"
