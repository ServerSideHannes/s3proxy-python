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

NAMESPACE="elasticsearch-test"
export CLUSTER_NAME="es-cluster"

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
# STEP 1: Create namespace and S3 credentials
# ============================================================================
log_info "=== Step 1: Creating namespace and credentials ==="
log_info "(ECK operator already installed by cluster up)"

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
            mc rm --recursive --force minio/es-snapshots/ 2>/dev/null || true
            echo 'Bucket cleaned'
        " 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl wait --namespace "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded pod/bucket-cleanup --timeout=60s 2>/dev/null || true
    kubectl delete pod -n "$NAMESPACE" bucket-cleanup --ignore-not-found >/dev/null 2>&1 || true
) &
BUCKET_CLEANUP_PID=$!

# Create S3 credentials secret for Elasticsearch
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/s3-credentials.yaml"

# ============================================================================
# STEP 2: Deploy Elasticsearch cluster (3 nodes)
# ============================================================================
log_info "=== Step 2: Deploying Elasticsearch cluster (3 nodes) ==="

envsubst < "${SCRIPT_DIR}/templates/elasticsearch-cluster.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Wait for bucket cleanup (must complete before snapshot creation)
wait $BUCKET_CLEANUP_PID || true

# ============================================================================
# STEP 3: Start esrally job (pre-built image, waits for ES then runs)
# ============================================================================
log_info "=== Step 3: Starting esrally job (using pre-built image) ==="

# Wait for ECK to create the password secret (wait for existence first, then data)
log_info "Waiting for ES password secret..."
until kubectl get secret -n "$NAMESPACE" ${CLUSTER_NAME}-es-elastic-user &>/dev/null; do
    echo "  Waiting for secret to be created..."
    sleep 2
done
kubectl wait --namespace "$NAMESPACE" \
    --for=jsonpath='{.data.elastic}' secret/${CLUSTER_NAME}-es-elastic-user \
    --timeout=120s

# Start esrally job NOW - it will do apt-get + pip install while ES is still starting
log_info "Starting esrally loader job..."
envsubst '$CLUSTER_NAME' < "${SCRIPT_DIR}/templates/esrally-job.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Follow esrally logs in background
kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod -l job-name=geonames-loader --timeout=120s 2>/dev/null || true
kubectl logs -n "$NAMESPACE" -f job/geonames-loader 2>/dev/null &
LOGS_PID=$!

# ============================================================================
# STEP 4: Wait for ES ready + register S3 repo
# ============================================================================
log_info "=== Step 4: Waiting for ES cluster ==="

kubectl wait --namespace "$NAMESPACE" \
    --for=jsonpath='{.status.phase}'=Ready elasticsearch/${CLUSTER_NAME} \
    --timeout=900s

log_info "Elasticsearch cluster is ready"

# Get ES password and register S3 repo
ES_PASSWORD=$(kubectl get secret -n "$NAMESPACE" ${CLUSTER_NAME}-es-elastic-user -o jsonpath='{.data.elastic}' | base64 -d)
ES_POD="${CLUSTER_NAME}-es-default-0"

log_info "Registering S3 snapshot repository..."
kubectl exec -n "$NAMESPACE" "${ES_POD}" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" -X PUT "https://localhost:9200/_snapshot/s3_backup" \
    -H "Content-Type: application/json" \
    -d '{
        "type": "s3",
        "settings": {
            "bucket": "elasticsearch-backups",
            "endpoint": "s3-gateway.s3proxy:80",
            "protocol": "http",
            "path_style_access": true
        }
    }'
echo ""
log_info "S3 snapshot repository registered"

# ============================================================================
# STEP 5: Wait for esrally to complete
# ============================================================================
log_info "=== Step 5: Waiting for esrally data loading ==="

kubectl wait --namespace "$NAMESPACE" \
    --for=condition=complete job/geonames-loader \
    --timeout=3600s

kill $LOGS_PID 2>/dev/null || true

# Check job status
JOB_STATUS=$(kubectl get job -n "$NAMESPACE" geonames-loader -o jsonpath='{.status.succeeded}')
if [ "$JOB_STATUS" != "1" ]; then
    log_error "Loader job failed!"
    kubectl logs -n "$NAMESPACE" job/geonames-loader --tail=50
    exit 1
fi

log_info "Data loading complete"

# Refresh index via kubectl exec (no port-forward needed)
kubectl exec -n "$NAMESPACE" "${CLUSTER_NAME}-es-default-0" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" -X POST "https://localhost:9200/geonames/_refresh" > /dev/null

# Get cluster stats via kubectl exec
log_info "Cluster stats:"
kubectl exec -n "$NAMESPACE" "${CLUSTER_NAME}-es-default-0" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" "https://localhost:9200/_cat/indices?v"
echo ""

TOTAL_DOCS=$(kubectl exec -n "$NAMESPACE" "${CLUSTER_NAME}-es-default-0" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" "https://localhost:9200/_cat/count?h=count" | tr -d '[:space:]')
log_info "Total documents: $TOTAL_DOCS"

# ============================================================================
# STEP 6: Create snapshot (backup)
# ============================================================================
log_info "=== Step 6: Creating snapshot ==="

SNAPSHOT_NAME="snapshot-$(date +%Y%m%d-%H%M%S)"

kubectl exec -n "$NAMESPACE" "${CLUSTER_NAME}-es-default-0" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" -X PUT "https://localhost:9200/_snapshot/s3_backup/${SNAPSHOT_NAME}?wait_for_completion=true" \
    -H "Content-Type: application/json" \
    -d '{
        "indices": "geonames",
        "ignore_unavailable": true,
        "include_global_state": false
    }'

echo ""
log_info "Snapshot ${SNAPSHOT_NAME} created"

# Get snapshot info
kubectl exec -n "$NAMESPACE" "${CLUSTER_NAME}-es-default-0" -- \
    curl -sk -u "elastic:${ES_PASSWORD}" "https://localhost:9200/_snapshot/s3_backup/${SNAPSHOT_NAME}" | jq .

# ============================================================================
# STEP 7: Verify encryption + Delete cluster + Create new cluster (ALL PARALLEL)
# ============================================================================
log_info "=== Step 7: Parallel - verify encryption, delete old, create new ==="

# 1. Start encryption verification in background
verify_encryption "elasticsearch-backups" "" "$NAMESPACE" "ALL" &
VERIFY_PID=$!

# 2. Delete old cluster in background
(
    kubectl delete elasticsearch -n "$NAMESPACE" ${CLUSTER_NAME} --wait
    kubectl wait --namespace "$NAMESPACE" --for=delete pod -l elasticsearch.k8s.elastic.co/cluster-name=${CLUSTER_NAME} --timeout=120s 2>/dev/null || true
    log_info "✓ Old cluster deleted"
) &
DELETE_PID=$!

# 3. Create new cluster immediately (different name, can coexist)
log_info "Creating restored cluster (parallel with deletion)..."
envsubst < "${SCRIPT_DIR}/templates/elasticsearch-cluster-restore.yaml" | kubectl apply -n "$NAMESPACE" -f -

# Wait for all parallel operations
wait $VERIFY_PID || { log_error "Encryption verification failed"; exit 1; }
log_info "✓ Encryption verified"

wait $DELETE_PID || { log_error "Old cluster deletion failed"; exit 1; }

log_info "Waiting for restored cluster to be ready..."
kubectl wait --namespace "$NAMESPACE" \
    --for=jsonpath='{.status.phase}'=Ready elasticsearch/${CLUSTER_NAME}-restored \
    --timeout=900s

# Get new password
NEW_ES_PASSWORD=$(kubectl get secret -n "$NAMESPACE" ${CLUSTER_NAME}-restored-es-elastic-user -o jsonpath='{.data.elastic}' | base64 -d)
RESTORED_POD="${CLUSTER_NAME}-restored-es-default-0"

# ============================================================================
# STEP 8: Register repository and restore snapshot
# ============================================================================
log_info "=== Step 8: Restoring from snapshot ==="

# Register repository via kubectl exec
kubectl exec -n "$NAMESPACE" "${RESTORED_POD}" -- \
    curl -sk -u "elastic:${NEW_ES_PASSWORD}" -X PUT "https://localhost:9200/_snapshot/s3_backup" \
    -H "Content-Type: application/json" \
    -d '{
        "type": "s3",
        "settings": {
            "bucket": "elasticsearch-backups",
            "endpoint": "s3-gateway.s3proxy:80",
            "protocol": "http",
            "path_style_access": true
        }
    }'

echo ""

# Restore snapshot
log_info "Restoring snapshot ${SNAPSHOT_NAME}..."
kubectl exec -n "$NAMESPACE" "${RESTORED_POD}" -- \
    curl -sk -u "elastic:${NEW_ES_PASSWORD}" -X POST "https://localhost:9200/_snapshot/s3_backup/${SNAPSHOT_NAME}/_restore?wait_for_completion=true" \
    -H "Content-Type: application/json" \
    -d '{
        "indices": "geonames",
        "ignore_unavailable": true,
        "include_global_state": false
    }'

echo ""
log_info "Restore complete!"

# ============================================================================
# STEP 9: Validate restored data
# ============================================================================
log_info "=== Step 9: Validating restored data ==="

# Wait for index to be green
kubectl exec -n "$NAMESPACE" "${RESTORED_POD}" -- \
    curl -sk -u "elastic:${NEW_ES_PASSWORD}" \
    "https://localhost:9200/_cluster/health/geonames?wait_for_status=green&timeout=60s" > /dev/null 2>&1 || true

log_info "Restored indices:"
kubectl exec -n "$NAMESPACE" "${RESTORED_POD}" -- \
    curl -sk -u "elastic:${NEW_ES_PASSWORD}" "https://localhost:9200/_cat/indices?v"
echo ""

RESTORED_DOCS=$(kubectl exec -n "$NAMESPACE" "${RESTORED_POD}" -- \
    curl -sk -u "elastic:${NEW_ES_PASSWORD}" "https://localhost:9200/_cat/count?h=count" | tr -d '[:space:]')
log_info "Total documents after restore: $RESTORED_DOCS"

# Validate counts
if [ "$TOTAL_DOCS" = "$RESTORED_DOCS" ]; then
    log_info "=== VALIDATION PASSED: Document counts match! ==="
    log_info "Original: $TOTAL_DOCS documents"
    log_info "Restored: $RESTORED_DOCS documents"
else
    log_error "=== VALIDATION FAILED: Document counts do not match! ==="
    log_error "Original: $TOTAL_DOCS documents"
    log_error "Restored: $RESTORED_DOCS documents"
    exit 1
fi

# ============================================================================
# STEP 10: Cleanup
# ============================================================================
log_info "=== Step 10: Cleanup ==="
log_info "Test completed successfully!"

log_info "=== Elasticsearch Backup/Restore Test PASSED ==="
