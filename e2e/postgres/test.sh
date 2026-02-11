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

NAMESPACE="postgres-test"
export CLUSTER_NAME="pg-cluster"
DATA_SIZE_GB=2
SCALE_FACTOR=$((DATA_SIZE_GB * 70))  # pgbench scale: ~15MB per scale factor

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false || true
}

trap cleanup EXIT

# ============================================================================
# STEP 1: Create namespace and S3 credentials
# ============================================================================
log_info "=== Step 1: Creating namespace and credentials ==="

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Clean up any leftover data in the S3 bucket from previous runs (background)
# CNPG's barman-cloud-check-wal-archive fails with "Expected empty archive" if bucket has old data
log_info "Cleaning S3 bucket from previous test runs (background)..."
(
    kubectl run bucket-cleanup --namespace "$NAMESPACE" \
        --image=mc:latest \
        --image-pull-policy=Never \
        --restart=Never \
        --command -- /bin/sh -c "
            mc alias set minio http://minio.minio.svc.cluster.local:9000 minioadmin minioadmin >/dev/null 2>&1
            mc rm --recursive --force minio/postgres-backups/ 2>/dev/null || true
            echo 'Bucket cleaned'
        " 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl delete pod -n "$NAMESPACE" bucket-cleanup --ignore-not-found >/dev/null 2>&1 || true
) &
BUCKET_CLEANUP_PID=$!

# Create S3 credentials secret
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/s3-credentials.yaml"

# ============================================================================
# STEP 2: Deploy PostgreSQL cluster (3 replicas)
# ============================================================================
log_info "=== Step 2: Deploying PostgreSQL cluster (3 replicas) ==="

envsubst < "${SCRIPT_DIR}/templates/postgres-cluster.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Wait for bucket cleanup before cluster tries to access S3
wait $BUCKET_CLEANUP_PID || true

log_info "Waiting for PostgreSQL cluster to be ready..."
kubectl wait --namespace "$NAMESPACE" \
    --for=condition=Ready cluster/${CLUSTER_NAME} \
    --timeout=600s

log_info "PostgreSQL cluster is ready"

# ============================================================================
# STEP 3: Generate test data using pgbench
# ============================================================================
log_info "=== Step 3: Generating ${DATA_SIZE_GB}GB of test data ==="

# Get the primary pod
PRIMARY_POD=$(kubectl get pods -n "$NAMESPACE" -l cnpg.io/cluster=${CLUSTER_NAME},role=primary -o jsonpath='{.items[0].metadata.name}')
log_info "Primary pod: $PRIMARY_POD"

# Initialize pgbench tables
log_info "Initializing pgbench with scale factor $SCALE_FACTOR (this may take a while)..."
kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- \
    pgbench -i -s "$SCALE_FACTOR" -U postgres app

# Run some transactions to generate more data
log_info "Running pgbench transactions..."
kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- \
    pgbench -U postgres -c 10 -j 2 -t 10000 app

# Create additional tables with fake data
log_info "Creating additional tables with fake data..."
kubectl exec -i -n "$NAMESPACE" "$PRIMARY_POD" -- psql -U postgres app <<'EOSQL'
-- Enable pgcrypto for gen_random_bytes()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create users table with fake data
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    data BYTEA
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    amount DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    metadata JSONB
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    price DECIMAL(10,2),
    inventory INTEGER,
    attributes JSONB
);

-- Insert fake users (with some random binary data for size)
INSERT INTO users (username, email, data)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    gen_random_bytes(1000)
FROM generate_series(1, 20000) AS i;

-- Insert fake orders
INSERT INTO orders (user_id, amount, status, metadata)
SELECT
    (random() * 20000)::int,
    (random() * 1000)::decimal(10,2),
    (ARRAY['pending', 'completed', 'shipped', 'cancelled'])[floor(random() * 4 + 1)],
    jsonb_build_object('source', 'web', 'version', floor(random() * 10))
FROM generate_series(1, 100000) AS i;

-- Insert fake products
INSERT INTO products (name, description, price, inventory, attributes)
SELECT
    'Product ' || i,
    'Description for product ' || i || '. ' || repeat('Lorem ipsum dolor sit amet. ', 10),
    (random() * 500)::decimal(10,2),
    (random() * 1000)::int,
    jsonb_build_object('category', 'cat_' || (i % 50), 'tags', ARRAY['tag1', 'tag2'])
FROM generate_series(1, 20000) AS i;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);

-- Analyze tables
ANALYZE;
EOSQL

# Get database size
DB_SIZE=$(kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- psql -U postgres -t -c "SELECT pg_size_pretty(pg_database_size('app'));")
log_info "Database size: $DB_SIZE"

# Update statistics and get row counts
log_info "Analyzing tables and getting row counts:"
kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- psql -U postgres app -c "ANALYZE;"
kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- psql -U postgres app -c "
SELECT
    schemaname || '.' || relname as table_name,
    n_live_tup as estimated_rows
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_live_tup DESC;
"

# Store checksum for validation
CHECKSUM=$(kubectl exec -n "$NAMESPACE" "$PRIMARY_POD" -- psql -U postgres -t -d app -c "
SELECT md5(string_agg(md5(row::text), ''))
FROM (
    SELECT * FROM users ORDER BY id LIMIT 1000
) row;
")
log_info "Data checksum (first 1000 users): $CHECKSUM"

# ============================================================================
# STEP 4: Trigger backup to S3
# ============================================================================
log_info "=== Step 4: Triggering backup to S3 ==="

envsubst < "${SCRIPT_DIR}/templates/backup.yaml" | kubectl apply -n "$NAMESPACE" -f -

log_info "Waiting for backup to complete..."
kubectl wait --namespace "$NAMESPACE" \
    --for=jsonpath='{.status.phase}'=completed backup/${CLUSTER_NAME}-backup-1 \
    --timeout=1800s

log_info "Backup completed!"
kubectl get backup -n "$NAMESPACE" ${CLUSTER_NAME}-backup-1 -o yaml | grep -A5 "status:"

# ============================================================================
# STEP 5: Verify encryption + Delete cluster + Create new cluster (ALL PARALLEL)
# ============================================================================
log_info "=== Step 5: Parallel - verify encryption, delete old, create new ==="

# 1. Start encryption verification in background
verify_encryption "postgres-backups" "" "$NAMESPACE" ".gz|.tar|.backup|.data" &
VERIFY_PID=$!

# 2. Delete old cluster in background
(
    kubectl delete cluster -n "$NAMESPACE" ${CLUSTER_NAME} --wait
    kubectl wait --namespace "$NAMESPACE" \
        --for=delete pod -l cnpg.io/cluster=${CLUSTER_NAME} \
        --timeout=300s || true
    log_info "✓ Old cluster deleted"
) &
DELETE_PID=$!

# 3. Create new cluster immediately (different name, can coexist)
log_info "Creating restored cluster (parallel with deletion)..."
envsubst < "${SCRIPT_DIR}/templates/postgres-cluster-restore.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Wait for all parallel operations
wait $VERIFY_PID || { log_error "Encryption verification failed"; exit 1; }
log_info "✓ Encryption verified"

wait $DELETE_PID || { log_error "Old cluster deletion failed"; exit 1; }

log_info "Waiting for restored cluster to be ready..."
kubectl wait --namespace "$NAMESPACE" \
    --for=condition=Ready cluster/${CLUSTER_NAME}-restored \
    --timeout=1800s

log_info "Restored cluster is ready!"

# ============================================================================
# STEP 6: Validate restored data
# ============================================================================
log_info "=== Step 6: Validating restored data ==="

RESTORED_PRIMARY=$(kubectl get pods -n "$NAMESPACE" -l cnpg.io/cluster=${CLUSTER_NAME}-restored,role=primary -o jsonpath='{.items[0].metadata.name}')
log_info "Restored primary pod: $RESTORED_PRIMARY"

# Get database size
RESTORED_DB_SIZE=$(kubectl exec -n "$NAMESPACE" "$RESTORED_PRIMARY" -- psql -U postgres -t -c "SELECT pg_size_pretty(pg_database_size('app'));")
log_info "Restored database size: $RESTORED_DB_SIZE"

# Update statistics and get row counts
log_info "Analyzing restored tables and getting row counts:"
kubectl exec -n "$NAMESPACE" "$RESTORED_PRIMARY" -- psql -U postgres app -c "ANALYZE;"
kubectl exec -n "$NAMESPACE" "$RESTORED_PRIMARY" -- psql -U postgres app -c "
SELECT
    schemaname || '.' || relname as table_name,
    n_live_tup as estimated_rows
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY n_live_tup DESC;
"

# Validate checksum
RESTORED_CHECKSUM=$(kubectl exec -n "$NAMESPACE" "$RESTORED_PRIMARY" -- psql -U postgres -t -d app -c "
SELECT md5(string_agg(md5(row::text), ''))
FROM (
    SELECT * FROM users ORDER BY id LIMIT 1000
) row;
")
log_info "Restored data checksum: $RESTORED_CHECKSUM"

if [ "$CHECKSUM" = "$RESTORED_CHECKSUM" ]; then
    log_info "=== VALIDATION PASSED: Checksums match! ==="
else
    log_error "=== VALIDATION FAILED: Checksums do not match! ==="
    log_error "Original: $CHECKSUM"
    log_error "Restored: $RESTORED_CHECKSUM"
    exit 1
fi

# ============================================================================
# STEP 7: Cleanup
# ============================================================================
log_info "=== Step 7: Cleanup ==="
log_info "Test completed successfully!"
log_info ""
log_info "The namespace $NAMESPACE will be deleted on script exit."
log_info "To keep the restored cluster for inspection, press Ctrl+C within 10 seconds..."
sleep 10

log_info "=== PostgreSQL Backup/Restore Test PASSED ==="
