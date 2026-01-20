#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="e2e/docker-compose.helm-test.yml"

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker compose -f $COMPOSE_FILE down -v 2>/dev/null || true
    docker rm -f s3proxy-test-control-plane 2>/dev/null || true
    docker network rm kind 2>/dev/null || true
}

trap cleanup EXIT INT TERM

echo "Starting cluster test (auto-cleanup on exit)..."
echo ""

docker compose -f $COMPOSE_FILE up --build -d

echo "Waiting for cluster..."
( docker compose -f $COMPOSE_FILE logs -f & ) | while read -r line; do
  echo "$line"
  if echo "$line" | grep -q "Cluster is ready"; then
    break
  fi
done

echo ""
echo "Running load test..."
echo ""

$SCRIPT_DIR/test-helm.sh load-test

echo ""
echo "✓ Tests passed!"
