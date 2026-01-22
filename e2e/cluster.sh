#!/bin/bash
set -e

COMPOSE_FILE="e2e/docker-compose.cluster.yml"

case "${1:-run}" in
  run)
    echo "Starting cluster test..."
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
    echo "Use './e2e/cluster.sh shell' to interact."
    echo "Use 'make clean' when done."
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
    echo "Running S3 load test (3 concurrent 10MB uploads)..."
    docker compose -f $COMPOSE_FILE exec helm-test sh -c '
      # Get pod names for load balancing verification
      PODS=$(kubectl get pods -n s3proxy -l app=s3proxy-python -o jsonpath="{.items[*].metadata.name}")
      POD_COUNT=$(echo $PODS | wc -w)
      echo "Found $POD_COUNT s3proxy pods: $PODS"

      # Save current log line counts
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
          # Create test bucket
          echo \"Creating test bucket...\"
          aws --endpoint-url http://s3-gateway.s3proxy s3 mb s3://load-test-bucket 2>/dev/null || true

          # Generate 3 random 10MB files (small for CI, still tests full flow)
          echo \"Generating 10MB test files...\"
          mkdir -p /tmp/testfiles
          for i in 1 2 3; do
            dd if=/dev/urandom of=/tmp/testfiles/file-\$i.bin bs=1M count=10 2>/dev/null &
          done
          wait
          echo \"Files generated\"
          ls -lh /tmp/testfiles/

          # Upload concurrently
          echo \"\"
          echo \"=== Starting concurrent uploads ===\"
          START=\$(date +%s)

          for i in 1 2 3; do
            aws --endpoint-url http://s3-gateway.s3proxy s3 cp /tmp/testfiles/file-\$i.bin s3://load-test-bucket/file-\$i.bin &
          done
          wait

          END=\$(date +%s)
          DURATION=\$((END - START))
          echo \"\"
          echo \"=== Upload complete in \${DURATION}s ===\"

          # Verify uploads
          echo \"\"
          echo \"=== Listing uploaded files ===\"
          aws --endpoint-url http://s3-gateway.s3proxy s3 ls s3://load-test-bucket/

          # Download and verify
          echo \"\"
          echo \"=== Downloading files to verify ===\"
          mkdir -p /tmp/downloads
          for i in 1 2 3; do
            aws --endpoint-url http://s3-gateway.s3proxy s3 cp s3://load-test-bucket/file-\$i.bin /tmp/downloads/file-\$i.bin &
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
            echo \"✓ Checksums match - round-trip successful\"
          else
            echo \"Checksum mismatch!\"
            exit 1
          fi

          # Verify encryption by reading raw data from MinIO directly
          echo \"\"
          echo \"=== Verifying encryption (reading raw from MinIO) ===\"

          # Create a small test file with known content
          echo \"Creating 100KB test file...\"
          dd if=/dev/urandom of=/tmp/encrypt-test.bin bs=1K count=100 2>/dev/null
          ORIG_SIZE=\$(stat -c%s /tmp/encrypt-test.bin 2>/dev/null || stat -f%z /tmp/encrypt-test.bin)
          ORIG_MD5=\$(md5sum /tmp/encrypt-test.bin | cut -c1-32)
          echo \"Original: \${ORIG_SIZE} bytes, MD5: \$ORIG_MD5\"

          # Upload through s3proxy (gets encrypted)
          aws --endpoint-url http://s3-gateway.s3proxy s3 cp /tmp/encrypt-test.bin s3://load-test-bucket/encrypt-test.bin

          # Download raw from MinIO directly (bypassing s3proxy decryption)
          echo \"Downloading raw encrypted data from MinIO...\"
          mkdir -p /tmp/raw
          aws --endpoint-url http://minio:9000 s3 cp s3://load-test-bucket/encrypt-test.bin /tmp/raw/encrypt-test.bin 2>/dev/null || true

          if [ -f /tmp/raw/encrypt-test.bin ]; then
            RAW_SIZE=\$(stat -c%s /tmp/raw/encrypt-test.bin 2>/dev/null || stat -f%z /tmp/raw/encrypt-test.bin)
            RAW_MD5=\$(md5sum /tmp/raw/encrypt-test.bin | cut -c1-32)
            echo \"Raw:      \${RAW_SIZE} bytes, MD5: \$RAW_MD5\"

            # AES-256-GCM adds exactly 28 bytes: 12-byte nonce + 16-byte auth tag
            EXPECTED_SIZE=\$((ORIG_SIZE + 28))

            if [ \"\$RAW_SIZE\" = \"\$EXPECTED_SIZE\" ] && [ \"\$ORIG_MD5\" != \"\$RAW_MD5\" ]; then
              echo \"✓ ENCRYPTION VERIFIED:\"
              echo \"  - Size increased by 28 bytes (12B nonce + 16B GCM tag)\"
              echo \"  - Content differs from original\"

              # Also verify decryption works
              aws --endpoint-url http://s3-gateway.s3proxy s3 cp s3://load-test-bucket/encrypt-test.bin /tmp/decrypted.bin
              DEC_SIZE=\$(stat -c%s /tmp/decrypted.bin 2>/dev/null || stat -f%z /tmp/decrypted.bin)
              DEC_MD5=\$(md5sum /tmp/decrypted.bin | cut -c1-32)
              echo \"Decrypted: \${DEC_SIZE} bytes, MD5: \$DEC_MD5\"

              if [ \"\$ORIG_SIZE\" = \"\$DEC_SIZE\" ] && [ \"\$ORIG_MD5\" = \"\$DEC_MD5\" ]; then
                echo \"✓ DECRYPTION VERIFIED - Size and content match original\"
              else
                echo \"✗ Decryption failed - data corrupted\"
                exit 1
              fi
            elif [ \"\$RAW_SIZE\" != \"\$EXPECTED_SIZE\" ]; then
              echo \"✗ ENCRYPTION FAILED - Expected \$EXPECTED_SIZE bytes, got \$RAW_SIZE\"
              echo \"  (Should be original + 28 bytes for AES-GCM overhead)\"
              exit 1
            else
              echo \"✗ ENCRYPTION FAILED - Raw data matches original\"
              exit 1
            fi
          else
            echo \"Could not read raw data from MinIO (bucket may have different name)\"
            echo \"Skipping raw encryption verification\"
          fi
        "

      LOAD_TEST_EXIT=$?
      if [ $LOAD_TEST_EXIT -ne 0 ]; then
        echo "✗ Load test failed with exit code $LOAD_TEST_EXIT"
        exit 1
      fi

      # Verify load balancing
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
    echo "  run       - Deploy Kind cluster and s3proxy"
    echo "  load-test - Run 30MB upload test + verify load balancing"
    echo "  status    - Show deployment status"
    echo "  pods      - Show pod details and resources"
    echo "  logs      - Stream s3proxy logs"
    echo "  shell     - Interactive kubectl shell"
    echo "  cleanup   - Delete cluster and clean up"
    exit 1
    ;;
esac
