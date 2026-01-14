#!/bin/bash
set -e

COMPOSE_FILE="e2e/docker-compose.helm-test.yml"

case "${1:-run}" in
  run)
    echo "Starting containerized Helm test..."
    echo ""
    # Start and follow logs until cluster is ready
    docker compose -f $COMPOSE_FILE up --build -d
    # Stream logs, exit when we see "Cluster is ready"
    ( docker compose -f $COMPOSE_FILE logs -f & ) | while read -r line; do
      echo "$line"
      if echo "$line" | grep -q "Cluster is ready"; then
        break
      fi
    done
    echo ""
    echo "=========================================="
    echo "Cluster is running in background."
    echo "Use 'make helm-shell' to interact."
    echo "Use 'make helm-cleanup' when done."
    echo "=========================================="
    ;;
  shell)
    echo "Opening shell in test container..."
    docker compose -f $COMPOSE_FILE exec helm-test sh
    ;;
  logs)
    echo "Showing pod logs..."
    docker compose -f $COMPOSE_FILE exec helm-test kubectl logs -l app=s3proxy-python -n s3proxy -f
    ;;
  status)
    echo "Checking deployment status..."
    docker compose -f $COMPOSE_FILE exec helm-test kubectl get all -n s3proxy
    ;;
  load-test)
    echo "Running S3 load test (3 concurrent 512MB uploads)..."
    docker compose -f $COMPOSE_FILE exec helm-test sh -c '
      echo "=== Creating test pod with AWS CLI ==="
      kubectl run s3-load-test -n s3proxy --rm -it --restart=Never \
        --image=amazon/aws-cli:latest \
        --env="AWS_ACCESS_KEY_ID=minioadmin" \
        --env="AWS_SECRET_ACCESS_KEY=minioadmin" \
        --env="AWS_DEFAULT_REGION=us-east-1" \
        --command -- /bin/sh -c "
          # Create test bucket
          echo \"Creating test bucket...\"
          aws --endpoint-url http://s3proxy-python:4433 s3 mb s3://load-test-bucket 2>/dev/null || true

          # Generate 3 random 512MB files
          echo \"Generating 512MB test files...\"
          mkdir -p /tmp/testfiles
          for i in 1 2 3; do
            dd if=/dev/urandom of=/tmp/testfiles/file-\$i.bin bs=1M count=512 2>/dev/null &
          done
          wait
          echo \"Files generated\"
          ls -lh /tmp/testfiles/

          # Upload concurrently
          echo \"\"
          echo \"=== Starting concurrent uploads ===\"
          START=\$(date +%s)

          for i in 1 2 3; do
            aws --endpoint-url http://s3proxy-python:4433 s3 cp /tmp/testfiles/file-\$i.bin s3://load-test-bucket/file-\$i.bin &
          done
          wait

          END=\$(date +%s)
          DURATION=\$((END - START))
          echo \"\"
          echo \"=== Upload complete in \${DURATION}s ===\"

          # Verify uploads
          echo \"\"
          echo \"=== Listing uploaded files ===\"
          aws --endpoint-url http://s3proxy-python:4433 s3 ls s3://load-test-bucket/

          # Download and verify
          echo \"\"
          echo \"=== Downloading files to verify ===\"
          mkdir -p /tmp/downloads
          for i in 1 2 3; do
            aws --endpoint-url http://s3proxy-python:4433 s3 cp s3://load-test-bucket/file-\$i.bin /tmp/downloads/file-\$i.bin &
          done
          wait

          echo \"\"
          echo \"=== Comparing checksums ===\"
          md5sum /tmp/testfiles/*.bin > /tmp/orig.md5
          md5sum /tmp/downloads/*.bin > /tmp/down.md5

          ORIG_SUMS=\$(cat /tmp/orig.md5 | while read sum name; do echo \$sum; done | sort)
          DOWN_SUMS=\$(cat /tmp/down.md5 | while read sum name; do echo \$sum; done | sort)

          cat /tmp/orig.md5
          echo \"\"
          if [ \"\$ORIG_SUMS\" = \"\$DOWN_SUMS\" ]; then
            echo \"All checksums match - encryption/decryption working!\"
          else
            echo \"Checksum mismatch!\"
            exit 1
          fi
        "
    '
    ;;
  watch)
    echo "Watching pod resource usage (Ctrl+C to stop)..."
    docker compose -f $COMPOSE_FILE exec helm-test sh -c '
      # Check if metrics-server is installed
      if ! kubectl get deployment metrics-server -n kube-system >/dev/null 2>&1; then
        echo "Installing metrics-server..."
        kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml >/dev/null 2>&1
        kubectl patch deployment metrics-server -n kube-system --type=json -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/args/-\", \"value\": \"--kubelet-insecure-tls\"}]" >/dev/null 2>&1
        echo "Waiting for metrics-server to be ready..."
        sleep 30
      fi
      # Loop to show live updates
      while true; do
        clear
        date
        echo ""
        kubectl top pods -n s3proxy 2>/dev/null || echo "Waiting for metrics..."
        sleep 2
      done
    '
    ;;
  redis)
    echo "Inspecting Redis state..."
    docker compose -f $COMPOSE_FILE exec helm-test sh -c '
      kubectl run redis-cli -n s3proxy --rm -it --restart=Never \
        --image=redis:7-alpine \
        --command -- sh -c "
          echo \"=== Redis Keys ===\"
          redis-cli -h s3proxy-redis-ha-haproxy KEYS \"*\"
          echo \"\"
          echo \"=== Redis Info ===\"
          redis-cli -h s3proxy-redis-ha-haproxy INFO keyspace
          redis-cli -h s3proxy-redis-ha-haproxy INFO memory | grep used_memory_human
          redis-cli -h s3proxy-redis-ha-haproxy INFO clients | grep connected_clients
        "
    '
    ;;
  pods)
    echo "Showing pod details..."
    docker compose -f $COMPOSE_FILE exec helm-test sh -c '
      echo "=== Pod Status ==="
      kubectl get pods -n s3proxy -o wide
      echo ""
      echo "=== Pod Resource Requests/Limits ==="
      kubectl get pods -n s3proxy -o custom-columns="NAME:.metadata.name,CPU_REQ:.spec.containers[0].resources.requests.cpu,CPU_LIM:.spec.containers[0].resources.limits.cpu,MEM_REQ:.spec.containers[0].resources.requests.memory,MEM_LIM:.spec.containers[0].resources.limits.memory"
      echo ""
      echo "=== Recent Events ==="
      kubectl get events -n s3proxy --sort-by=.lastTimestamp | tail -10
    '
    ;;
  cleanup)
    echo "Cleaning up..."
    # Stop compose containers
    docker compose -f $COMPOSE_FILE down -v 2>/dev/null || true
    # Delete Kind cluster containers directly
    docker rm -f s3proxy-test-control-plane 2>/dev/null || true
    # Clean up Kind network
    docker network rm kind 2>/dev/null || true
    echo "Cleanup complete"
    ;;
  *)
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  run        - Deploy Kind cluster and Helm chart"
    echo "  status     - Show deployment status"
    echo "  pods       - Show pod details and resources"
    echo "  logs       - Stream s3proxy logs"
    echo "  load-test  - Run 1.5GB concurrent upload test"
    echo "  redis      - Inspect Redis keys and memory"
    echo "  watch      - Live pod CPU/memory (installs metrics-server)"
    echo "  shell      - Interactive kubectl shell"
    echo "  cleanup    - Delete cluster and clean up"
    exit 1
    ;;
esac
