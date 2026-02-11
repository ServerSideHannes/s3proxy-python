#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_FILE="docker-compose.yml"

case "${1:-help}" in
  up)
    echo "=== Starting database test cluster ==="
    docker compose -f $COMPOSE_FILE up --build -d

    echo "Waiting for cluster to be ready..."
    ( docker compose -f $COMPOSE_FILE logs -f & ) | while read -r line; do
      echo "$line"
      if echo "$line" | grep -q "Cluster is ready"; then
        break
      fi
    done

    echo ""
    echo "=========================================="
    echo "Cluster is running in background."
    echo ""
    echo "Run tests:"
    echo "  ./cluster.sh postgres"
    echo "  ./cluster.sh elasticsearch"
    echo "  ./cluster.sh scylla"
    echo ""
    echo "Or open a shell:"
    echo "  ./cluster.sh shell"
    echo ""
    echo "Cleanup when done:"
    echo "  ./cluster.sh down"
    echo "=========================================="
    ;;

  postgres)
    echo "=== Running PostgreSQL (CloudNativePG) test ==="
    docker compose -f $COMPOSE_FILE exec db-test ./postgres/test.sh
    ;;

  elasticsearch|es)
    echo "=== Running Elasticsearch (ECK) test ==="
    docker compose -f $COMPOSE_FILE exec db-test ./elasticsearch/test.sh
    ;;

  scylla)
    echo "=== Running ScyllaDB test ==="
    docker compose -f $COMPOSE_FILE exec db-test ./scylla/test.sh
    ;;

  clickhouse|ch)
    echo "=== Running ClickHouse test ==="
    docker compose -f $COMPOSE_FILE exec db-test ./clickhouse/test.sh
    ;;

  s3-compat|s3)
    echo "=== Running S3 Compatibility (Ceph s3-tests) ==="
    docker compose -f $COMPOSE_FILE exec db-test ./s3-compatibility/test.sh
    ;;

  all)
    echo "=== Running all database tests ==="
    docker compose -f $COMPOSE_FILE exec db-test ./postgres/test.sh
    docker compose -f $COMPOSE_FILE exec db-test ./elasticsearch/test.sh
    docker compose -f $COMPOSE_FILE exec db-test ./scylla/test.sh
    docker compose -f $COMPOSE_FILE exec db-test ./clickhouse/test.sh
    echo "=== All tests completed ==="
    ;;

  load-test)
    echo "=== Running S3 load test (3 concurrent 10MB uploads) ==="
    docker compose -f $COMPOSE_FILE exec db-test sh -c '
      PODS=$(kubectl get pods -n s3proxy -l app=s3proxy-python -o jsonpath="{.items[*].metadata.name}")
      POD_COUNT=$(echo $PODS | wc -w)
      echo "Found $POD_COUNT s3proxy pods: $PODS"

      mkdir -p /tmp/lb-test
      for pod in $PODS; do
        kubectl logs $pod -n s3proxy 2>/dev/null | wc -l > /tmp/lb-test/$pod.start
      done

      echo "=== Creating test pod with AWS CLI ==="
      kubectl run s3-load-test -n s3proxy --rm -i --restart=Never \
        --image=amazon/aws-cli:latest \
        --env="AWS_ACCESS_KEY_ID=minioadmin" \
        --env="AWS_SECRET_ACCESS_KEY=minioadmin" \
        --env="AWS_DEFAULT_REGION=us-east-1" \
        --command -- /bin/sh -c "
          aws --endpoint-url http://s3-gateway.s3proxy s3 mb s3://load-test-bucket 2>/dev/null || true

          echo \"Generating 10MB test files...\"
          mkdir -p /tmp/testfiles
          for i in 1 2 3; do
            dd if=/dev/urandom of=/tmp/testfiles/file-\$i.bin bs=1M count=10 2>/dev/null &
          done
          wait
          ls -lh /tmp/testfiles/

          echo \"=== Starting concurrent uploads ===\"
          START=\$(date +%s)
          for i in 1 2 3; do
            aws --endpoint-url http://s3-gateway.s3proxy s3 cp /tmp/testfiles/file-\$i.bin s3://load-test-bucket/file-\$i.bin &
          done
          wait
          END=\$(date +%s)
          echo \"Upload complete in \$((END - START))s\"

          echo \"=== Verifying uploads ===\"
          aws --endpoint-url http://s3-gateway.s3proxy s3 ls s3://load-test-bucket/

          echo \"=== Downloading and verifying ===\"
          mkdir -p /tmp/downloads
          for i in 1 2 3; do
            aws --endpoint-url http://s3-gateway.s3proxy s3 cp s3://load-test-bucket/file-\$i.bin /tmp/downloads/file-\$i.bin &
          done
          wait

          md5sum /tmp/testfiles/*.bin > /tmp/orig.md5
          md5sum /tmp/downloads/*.bin > /tmp/down.md5
          ORIG_SUMS=\$(cat /tmp/orig.md5 | while read sum name; do echo \$sum; done | sort)
          DOWN_SUMS=\$(cat /tmp/down.md5 | while read sum name; do echo \$sum; done | sort)

          if [ \"\$ORIG_SUMS\" = \"\$DOWN_SUMS\" ]; then
            echo \"✓ Checksums match - round-trip successful\"
          else
            echo \"✗ Checksum mismatch!\"
            exit 1
          fi
        "

      echo ""
      echo "=== Checking load balancing ==="
      sleep 2
      PODS_HIT=0
      for pod in $PODS; do
        START_LINE=$(cat /tmp/lb-test/$pod.start 2>/dev/null || echo "0")
        REQUEST_COUNT=$(kubectl logs $pod -n s3proxy 2>/dev/null | tail -n +$((START_LINE + 1)) | grep -c -E "GET|POST|PUT|HEAD" || echo "0")
        if [ "$REQUEST_COUNT" -gt 0 ]; then
          PODS_HIT=$((PODS_HIT + 1))
          echo "✓ Pod $pod: received $REQUEST_COUNT requests"
        else
          echo "  Pod $pod: received 0 requests"
        fi
      done
      rm -rf /tmp/lb-test

      if [ "$PODS_HIT" -ge 2 ]; then
        echo "✓ Load balancing verified - traffic distributed across $PODS_HIT pods"
      else
        echo "⚠ Traffic went to only $PODS_HIT pod(s)"
      fi
    '
    ;;

  down)
    echo "=== Cleaning up ==="
    docker compose -f $COMPOSE_FILE down 2>/dev/null || true
    # Remove all e2e volumes EXCEPT registry cache
    docker volume ls -q --filter name=e2e_ | grep -v e2e_registry-data | xargs -r docker volume rm 2>/dev/null || true
    # Remove anonymous volumes (64-char hex names from Kind/Docker)
    docker volume ls -q | grep -E '^[a-f0-9]{64}$' | xargs -r docker volume rm 2>/dev/null || true
    # Delete Kind cluster containers directly
    docker rm -f db-backup-test-control-plane db-backup-test-worker db-backup-test-worker2 db-backup-test-worker3 2>/dev/null || true
    # Clean up Kind network
    docker network rm kind 2>/dev/null || true
    echo "Cleanup complete (registry cache preserved)"
    ;;

  *)
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  up             - Start Kind cluster + s3proxy + MinIO"
    echo "  down           - Stop and cleanup everything"
    echo "  status         - Show cluster status"
    echo "  logs           - Show cluster logs"
    echo "  shell          - Open shell in test container"
    echo ""
    echo "Tests:"
    echo "  load-test      - Run S3 load test (upload/download verification)"
    echo "  s3-compat      - Run S3 compatibility tests (Ceph s3-tests)"
    echo "  postgres       - Run PostgreSQL (CloudNativePG) backup test"
    echo "  elasticsearch  - Run Elasticsearch (ECK) backup test"
    echo "  scylla         - Run ScyllaDB backup test"
    echo "  clickhouse     - Run ClickHouse backup test"
    echo "  all            - Run all database backup tests"
    echo ""
    echo "Example:"
    echo "  ./cluster.sh up"
    echo "  ./cluster.sh load-test"
    echo "  ./cluster.sh down"
    echo ""
    exit 1
    ;;
esac
