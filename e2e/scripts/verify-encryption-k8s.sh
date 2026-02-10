#!/usr/bin/env bash
# Kubernetes wrapper for encryption verification
# Source this script and call: verify_encryption <bucket> <path-prefix> <namespace>

verify_encryption() {
    local BUCKET="$1"
    local PATH_PREFIX="${2:-}"
    local NAMESPACE="${3:-default}"

    kubectl run encryption-check --namespace "$NAMESPACE" \
        --image=mc:latest \
        --image-pull-policy=Never \
        --restart=Never \
        --command -- /bin/sh -c "
            set -e
            echo '[1/3] Connecting to MinIO...'
            timeout 30 mc alias set minio http://minio.minio.svc.cluster.local:9000 minioadmin minioadmin >/dev/null 2>&1
            echo '      ✓ Connected to MinIO'

            echo '[2/3] Listing backup files...'
            echo ''
            echo '=== Encryption Verification ==='
            echo 'Bucket: $BUCKET'
            echo 'Path:   ${PATH_PREFIX:-<root>}'
            echo ''

            # List ALL files, excluding .s3proxy-internal/ metadata
            FILES=\$(timeout 60 mc ls -r minio/$BUCKET/${PATH_PREFIX} 2>/dev/null | awk '{print \$NF}' | grep -v '^\.s3proxy-internal/' || true)
            COUNT=\$(echo \"\$FILES\" | grep -c . || echo 0)

            echo \"      ✓ Found \$COUNT files to verify\"
            [ \"\$COUNT\" -eq 0 ] && { echo '✗ No files found!'; exit 1; }

            echo '[3/3] Verifying encryption (magic byte + entropy check)...'
            CHECKED=0 PASSED=0 FAILED=0 SKIPPED=0
            FAILED_FILES='' SKIPPED_FILES=''

            for F in \$FILES; do
                [ -z \"\$F\" ] && continue

                FAIL_REASON=''

                # Entropy check - encrypted data should have high entropy
                # Stream only first 4KB instead of downloading entire file
                if ! timeout 30 mc cat \"minio/$BUCKET/${PATH_PREFIX}\$F\" 2>/dev/null | head -c 4096 > /tmp/f; then
                    SKIPPED=\$((SKIPPED + 1))
                    SKIPPED_FILES=\"\${SKIPPED_FILES}  - \$F (download failed)\n\"
                    continue
                fi

                SIZE=\$(stat -c%s /tmp/f 2>/dev/null || stat -f%z /tmp/f)
                if [ \"\$SIZE\" -lt 100 ]; then
                    SKIPPED=\$((SKIPPED + 1))
                    SKIPPED_FILES=\"\${SKIPPED_FILES}  - \$F (too small: \${SIZE} bytes)\n\"
                    rm -f /tmp/f
                    continue
                fi

                CHECKED=\$((CHECKED + 1))

                # Entropy check - encrypted data should have high entropy (>6.0 bits/byte)
                ENT=\$(cat /tmp/f | od -A n -t u1 | tr ' ' '\n' | grep -v '^\$' | sort | uniq -c | awk '
                    BEGIN{t=0;e=0}{c[\$2]=\$1;t+=\$1}END{for(b in c){p=c[b]/t;if(p>0)e-=p*log(p)/log(2)}printf\"%.2f\",e}')
                rm -f /tmp/f

                if awk \"BEGIN{exit!(\$ENT<6.0)}\"; then
                    [ -n \"\$FAIL_REASON\" ] && FAIL_REASON=\"\$FAIL_REASON + \"
                    FAIL_REASON=\"\${FAIL_REASON}low entropy: \$ENT\"
                fi

                if [ -n \"\$FAIL_REASON\" ]; then
                    FAILED=\$((FAILED + 1))
                    FAILED_FILES=\"\${FAILED_FILES}  ✗ \$F (\$FAIL_REASON)\n\"
                else
                    PASSED=\$((PASSED + 1))
                fi

                [ \$((CHECKED % 10)) -eq 0 ] && echo \"      Progress: \$CHECKED/\$COUNT files checked (Encrypted: \$PASSED, Unencrypted: \$FAILED, Skipped: \$SKIPPED)\"
            done

            echo ''
            echo '=== SUMMARY ==='
            echo \"Total found: \$COUNT\"
            echo \"Checked:     \$CHECKED\"
            echo \"Encrypted:   \$PASSED\"
            echo \"Unencrypted: \$FAILED\"
            echo \"Skipped:     \$SKIPPED\"
            echo ''

            if [ \"\$SKIPPED\" -gt 0 ]; then
                echo ''; echo 'SKIPPED FILES:'; echo -e \"\$SKIPPED_FILES\"
            fi

            if [ \"\$FAILED\" -gt 0 ]; then
                echo ''; echo 'UNENCRYPTED FILES:'; echo -e \"\$FAILED_FILES\"
                echo '✗ ENCRYPTION VERIFICATION FAILED!'
                exit 1
            fi

            echo '✓ ALL FILES ENCRYPTED'
        "

    kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod/encryption-check --timeout=60s || true
    # Wait for pod to complete (Succeeded or Failed), not just be Ready
    # Poll for completion instead of sequential waits to avoid long timeouts on failure
    for i in $(seq 1 60); do
        PHASE=$(kubectl get pod -n "$NAMESPACE" encryption-check -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        if [ "$PHASE" = "Succeeded" ] || [ "$PHASE" = "Failed" ]; then
            break
        fi
        sleep 5
    done
    kubectl logs -n "$NAMESPACE" encryption-check || true
    local EXIT_CODE=$(kubectl get pod -n "$NAMESPACE" encryption-check -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo "")
    kubectl delete pod -n "$NAMESPACE" encryption-check --ignore-not-found >/dev/null 2>&1

    if [ -z "$EXIT_CODE" ]; then
        echo "ERROR: Could not determine pod exit code (pod may not have terminated)"
        return 1
    fi
    if [ "$EXIT_CODE" != "0" ]; then
        echo "Encryption verification failed with exit code: $EXIT_CODE"
        return 1
    fi
    return 0
}
