#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="e2e/docker-compose.helm-test.yml"

echo "Starting containerized Helm test with load-test..."
echo "This will start the cluster, run load tests, then cleanup"
echo ""

# Start cluster in detached mode
docker compose -f $COMPOSE_FILE up --build -d

# Stream logs until cluster is ready
echo "Waiting for cluster to be ready..."
( docker compose -f $COMPOSE_FILE logs -f & ) | while read -r line; do
  echo "$line"
  if echo "$line" | grep -q "Cluster is ready"; then
    break
  fi
done

echo ""
echo "=========================================="
echo "Running load test..."
echo "=========================================="
echo ""

# Run load test using the shared script
$SCRIPT_DIR/test-helm.sh load-test
TEST_EXIT=$?

echo ""
echo "=========================================="
echo "Cleaning up..."
echo "=========================================="
echo ""

docker compose -f $COMPOSE_FILE down -v 2>&1 | grep -v "Resource is still in use"

if [ $TEST_EXIT -eq 0 ]; then
  echo ""
  echo "✓ All tests completed successfully!"
  exit 0
else
  echo ""
  echo "✗ Tests failed!"
  exit 1
fi
