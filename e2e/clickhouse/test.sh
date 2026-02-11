#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# Source shared encryption verification
source "${SCRIPT_DIR}/../scripts/verify-encryption-k8s.sh"

# Use isolated kubeconfig if not already set (running outside container)
if [ -z "${KUBECONFIG:-}" ]; then
    export KUBECONFIG="${ROOT_DIR}/kubeconfig"
    if [ ! -f "$KUBECONFIG" ]; then
        echo "ERROR: Kubeconfig not found at $KUBECONFIG"
        echo "Run ./cluster.sh up first"
        exit 1
    fi
fi

NAMESPACE="clickhouse-test"
export CLUSTER_NAME="chi-backup-test"
DATA_SIZE_ROWS=50000000  # 50M rows

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false || true
}

trap cleanup EXIT

# ============================================================================
# STEP 1: Create namespace and clickhouse-backup config
# ============================================================================
log_info "=== Step 1: Creating namespace and clickhouse-backup config ==="

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Clean up any leftover data in the S3 bucket from previous runs (background)
log_info "Cleaning S3 bucket from previous test runs (background)..."
(
    kubectl run bucket-cleanup --namespace "$NAMESPACE" \
        --image=mc:latest \
        --image-pull-policy=Never \
        --restart=Never \
        --command -- /bin/sh -c "
            mc alias set minio http://minio.minio.svc.cluster.local:9000 minioadmin minioadmin >/dev/null 2>&1
            mc rm --recursive --force minio/clickhouse-backups/ 2>/dev/null || true
            echo 'Bucket cleaned'
        " 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl delete pod -n "$NAMESPACE" bucket-cleanup --ignore-not-found >/dev/null 2>&1 || true
) &
BUCKET_CLEANUP_PID=$!

# Create clickhouse-backup config
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/backup-config.yaml"

# ============================================================================
# STEP 2: Deploy BOTH ClickHouse clusters (source + restore target) in parallel
# ============================================================================
log_info "=== Step 2: Deploying BOTH ClickHouse clusters (source + restore) ==="

export RESTORE_CLUSTER_NAME="${CLUSTER_NAME}-restore"

# Deploy source cluster
envsubst < "${SCRIPT_DIR}/templates/clickhouse-installation.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Deploy restore target cluster (same spec, different name)
envsubst < "${SCRIPT_DIR}/templates/clickhouse-installation-restore.yaml" | kubectl apply -n "$NAMESPACE" -f -

log_info "Waiting for BOTH ClickHouse clusters to be ready (parallel)..."

# Wait for source cluster pods
(
    until kubectl get pods -n "$NAMESPACE" -l "clickhouse.altinity.com/chi=${CLUSTER_NAME}" --no-headers 2>/dev/null | grep -q .; do
        sleep 5
    done
    kubectl wait --namespace "$NAMESPACE" --for=condition=ready pod \
        --selector="clickhouse.altinity.com/chi=${CLUSTER_NAME}" --timeout=600s
    echo "✓ Source cluster ready"
) &
SOURCE_WAIT_PID=$!

# Wait for restore cluster pods
(
    until kubectl get pods -n "$NAMESPACE" -l "clickhouse.altinity.com/chi=${RESTORE_CLUSTER_NAME}" --no-headers 2>/dev/null | grep -q .; do
        sleep 5
    done
    kubectl wait --namespace "$NAMESPACE" --for=condition=ready pod \
        --selector="clickhouse.altinity.com/chi=${RESTORE_CLUSTER_NAME}" --timeout=600s
    echo "✓ Restore cluster ready"
) &
RESTORE_WAIT_PID=$!

# Wait for bucket cleanup (must complete before backup)
wait $BUCKET_CLEANUP_PID || true

wait $SOURCE_WAIT_PID || { log_error "Source cluster failed to start"; exit 1; }
wait $RESTORE_WAIT_PID || { log_error "Restore cluster failed to start"; exit 1; }

log_info "Both ClickHouse clusters are ready"

# Get the first pod name
CH_POD=$(kubectl get pods -n "$NAMESPACE" -l "clickhouse.altinity.com/chi=${CLUSTER_NAME}" -o jsonpath='{.items[0].metadata.name}')
log_info "Using ClickHouse pod: $CH_POD"

# Verify clickhouse-backup is working
log_info "Verifying clickhouse-backup installation..."
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse-backup -- clickhouse-backup --version

# ============================================================================
# STEP 3: Generate test data using ClickHouse built-in functions
# ============================================================================
log_info "=== Step 3: Generating ${DATA_SIZE_ROWS} rows of test data ==="

# Create database
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test_db;"

# Create events table with various data types
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
CREATE TABLE IF NOT EXISTS test_db.events (
    id UInt64,
    event_time DateTime64(3) DEFAULT now64(3),
    user_id UInt32,
    event_type LowCardinality(String),
    page_url String,
    referrer String,
    ip_address IPv4,
    user_agent String,
    country_code LowCardinality(FixedString(2)),
    amount Decimal64(2),
    metadata String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id, id);
"

log_info "Inserting ${DATA_SIZE_ROWS} rows using generateRandom()..."
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
INSERT INTO test_db.events (id, event_time, user_id, event_type, page_url, referrer, ip_address, user_agent, country_code, amount, metadata)
SELECT
    number as id,
    now64(3) - toIntervalSecond(rand() % 31536000) as event_time,
    rand() % 1000000 as user_id,
    arrayElement(['click', 'view', 'purchase', 'signup', 'logout'], (rand() % 5) + 1) as event_type,
    concat('https://example.com/page/', toString(rand() % 10000)) as page_url,
    concat('https://referrer.com/', toString(rand() % 1000)) as referrer,
    toIPv4(rand()) as ip_address,
    concat('Mozilla/5.0 (', arrayElement(['Windows', 'Mac', 'Linux', 'iOS', 'Android'], (rand() % 5) + 1), ')') as user_agent,
    arrayElement(['US', 'GB', 'DE', 'FR', 'JP', 'CN', 'BR', 'IN', 'CA', 'AU'], (rand() % 10) + 1) as country_code,
    round(rand() % 100000 / 100, 2) as amount,
    concat('{\"session_id\":\"', toString(generateUUIDv4()), '\",\"version\":', toString(rand() % 10), '}') as metadata
FROM numbers(${DATA_SIZE_ROWS});
"

# Create another table for aggregated data
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
CREATE TABLE IF NOT EXISTS test_db.daily_stats (
    date Date,
    country_code LowCardinality(FixedString(2)),
    event_type LowCardinality(String),
    total_events UInt64,
    unique_users UInt64,
    total_amount Decimal128(2)
) ENGINE = SummingMergeTree()
ORDER BY (date, country_code, event_type);
"

# Insert aggregated data
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
INSERT INTO test_db.daily_stats
SELECT
    toDate(event_time) as date,
    country_code,
    event_type,
    count() as total_events,
    uniqExact(user_id) as unique_users,
    sum(amount) as total_amount
FROM test_db.events
GROUP BY date, country_code, event_type;
"

log_info "Data generation complete"

# Get table stats
log_info "Table statistics:"
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
SELECT
    database,
    table,
    formatReadableQuantity(sum(rows)) as rows,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE active AND database = 'test_db'
GROUP BY database, table
FORMAT Pretty;
"

# Get checksum for validation
CHECKSUM=$(kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse -- clickhouse-client --query "
SELECT cityHash64(groupArray(id)) FROM (SELECT id FROM test_db.events ORDER BY id LIMIT 10000);
")
log_info "Data checksum (first 10k rows): $CHECKSUM"

# ============================================================================
# STEP 4: Create backup using clickhouse-backup
# ============================================================================
log_info "=== Step 4: Creating backup to S3 using clickhouse-backup ==="

BACKUP_NAME="backup_$(date +%Y%m%d_%H%M%S)"

# Create local backup first, then upload to S3
log_info "Creating local backup..."
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse-backup -- clickhouse-backup create "$BACKUP_NAME"

log_info "Uploading backup to S3..."
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse-backup -- clickhouse-backup upload "$BACKUP_NAME"

log_info "Backup ${BACKUP_NAME} created and uploaded"

# List remote backups
log_info "Remote backups:"
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse-backup -- clickhouse-backup list remote

# Delete local backup to save space
kubectl exec -n "$NAMESPACE" "$CH_POD" -c clickhouse-backup -- clickhouse-backup delete local "$BACKUP_NAME"

# ============================================================================
# STEP 5 + 6: Verify encryption + Restore to pre-created cluster (parallel)
# ============================================================================
log_info "=== Step 5 + 6: Restore to pre-created cluster (parallel with encryption verification) ==="

# Get the restore cluster pod (already running!)
RESTORE_POD=$(kubectl get pods -n "$NAMESPACE" -l "clickhouse.altinity.com/chi=${RESTORE_CLUSTER_NAME}" -o jsonpath='{.items[0].metadata.name}')
log_info "Using restore cluster pod: $RESTORE_POD (already running)"

# Start encryption verification in background (runs throughout restore)
verify_encryption "clickhouse-backups" "backups/" "$NAMESPACE" &
VERIFY_PID=$!

# Restore to the PRE-CREATED restore cluster (no waiting for cluster to start!)
log_info "Downloading backup to restore cluster..."
kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse-backup -- clickhouse-backup download "$BACKUP_NAME"

log_info "Restoring backup to restore cluster..."
kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse-backup -- clickhouse-backup restore "$BACKUP_NAME"
log_info "✓ Restore complete"

# Clean up local backup on restore cluster
kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse-backup -- clickhouse-backup delete local "$BACKUP_NAME"

# Now wait for encryption verification
wait $VERIFY_PID || { log_error "Encryption verification failed"; exit 1; }
log_info "✓ Encryption verified"

# ============================================================================
# STEP 7: Validate restored data on RESTORE cluster
# ============================================================================
log_info "=== Step 7: Validating restored data on restore cluster ==="

# Get table stats after restore
log_info "Restored table statistics:"
kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse -- clickhouse-client --query "
SELECT
    database,
    table,
    formatReadableQuantity(sum(rows)) as rows,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE active AND database = 'test_db'
GROUP BY database, table
FORMAT Pretty;
"

# Get row count from RESTORE cluster
RESTORED_COUNT=$(kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse -- clickhouse-client --query "
SELECT count() FROM test_db.events;
")
log_info "Restored row count: $RESTORED_COUNT"

# Get checksum for validation from RESTORE cluster
RESTORED_CHECKSUM=$(kubectl exec -n "$NAMESPACE" "$RESTORE_POD" -c clickhouse -- clickhouse-client --query "
SELECT cityHash64(groupArray(id)) FROM (SELECT id FROM test_db.events ORDER BY id LIMIT 10000);
")
log_info "Restored data checksum: $RESTORED_CHECKSUM"

# Validate
if [ "$CHECKSUM" = "$RESTORED_CHECKSUM" ]; then
    log_info "=== VALIDATION PASSED: Checksums match! ==="
    log_info "Original: $CHECKSUM"
    log_info "Restored: $RESTORED_CHECKSUM"
else
    log_error "=== VALIDATION FAILED: Checksums do not match! ==="
    log_error "Original: $CHECKSUM"
    log_error "Restored: $RESTORED_CHECKSUM"
    exit 1
fi

if [ "$RESTORED_COUNT" = "$DATA_SIZE_ROWS" ]; then
    log_info "=== ROW COUNT PASSED: $RESTORED_COUNT rows ==="
else
    log_error "=== ROW COUNT MISMATCH: Expected $DATA_SIZE_ROWS, got $RESTORED_COUNT ==="
    exit 1
fi

# ============================================================================
# STEP 8: Cleanup
# ============================================================================
log_info "=== Step 8: Cleanup ==="
log_info "Test completed successfully!"
log_info ""
log_info "The namespace $NAMESPACE will be deleted on script exit."
log_info "To keep the cluster for inspection, press Ctrl+C within 10 seconds..."
sleep 10

log_info "=== ClickHouse Backup/Restore Test PASSED ==="
