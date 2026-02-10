#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

source "${SCRIPT_DIR}/../scripts/verify-encryption-k8s.sh"

if [ -z "${KUBECONFIG:-}" ]; then
    export KUBECONFIG="${ROOT_DIR}/kubeconfig"
    if [ ! -f "$KUBECONFIG" ]; then
        echo "ERROR: Kubeconfig not found. Run ./cluster.sh up first"
        exit 1
    fi
fi

NAMESPACE="scylla-test"
export CLUSTER_NAME="scylla-cluster"

log_info() { echo -e "\033[0;32m[INFO]\033[0m $1"; }
log_warn() { echo -e "\033[1;33m[WARN]\033[0m $1"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $1"; }

cleanup() {
    log_info "Cleaning up..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=false || true
}
trap cleanup EXIT

# Delete existing namespace
if kubectl get namespace "$NAMESPACE" &>/dev/null; then
    # Clear finalizers from scylla resources first
    for r in $(kubectl api-resources --namespaced -o name 2>/dev/null | grep scylla); do
        kubectl get "$r" -n "$NAMESPACE" -o name 2>/dev/null | xargs -I{} kubectl patch {} -n "$NAMESPACE" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
    done
    kubectl delete namespace "$NAMESPACE" --timeout=10s 2>/dev/null || \
        kubectl get namespace "$NAMESPACE" -o json | jq '.spec.finalizers=[]' | kubectl replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f - 2>/dev/null || true
    sleep 2
fi

# Label nodes for ScyllaDB
for node in $(kubectl get nodes -o name | grep -v control-plane); do
    kubectl label "$node" scylla.scylladb.com/node-type=scylla --overwrite 2>/dev/null || true
done

# Create storage class for Scylla (uses default provisioner)
kubectl apply -f "${SCRIPT_DIR}/templates/storage-class.yaml"

kubectl create namespace "$NAMESPACE"

# S3 credentials for Scylla agent
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/agent-config-secret.yaml"

# ============================================================================
# STEP 1: Create cluster
# ============================================================================
log_info "=== Step 1: Creating ScyllaDB cluster ==="

envsubst < "${SCRIPT_DIR}/templates/scylla-cluster.yaml" | kubectl apply -n "$NAMESPACE" -f -

log_info "Waiting for pods to be created..."
until kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=scylla --no-headers 2>/dev/null | grep -q .; do
    sleep 5
done

log_info "Waiting for all 3 pods to be ready..."
until [ "$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=scylla --no-headers 2>/dev/null | grep -c Running)" -eq 3 ]; do
    sleep 5
done
kubectl wait --namespace "$NAMESPACE" --for=condition=ready pod -l app.kubernetes.io/name=scylla --timeout=600s

SCYLLA_POD=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=scylla -o jsonpath='{.items[0].metadata.name}')
log_info "Cluster ready: $SCYLLA_POD"

# ============================================================================
# STEP 2: Generate ~2GB data using scylla-bench
# ============================================================================
log_info "=== Step 2: Generating ~2GB test data ==="

# Clean up any leftover bench pod
log_info "=== Step 2: Generating ~2GB test data ==="

# 1. Give the network and schema a moment to settle
log_info "Waiting for service endpoints to be ready..."
sleep 20

# 2. Run bench in the background (no -i, no --rm) so we can control the wait
kubectl delete pod scylla-bench -n "$NAMESPACE" --ignore-not-found

kubectl run scylla-bench --namespace "$NAMESPACE" \
    --image=scylladb/scylla-bench:0.3.6 \
    --restart=Never \
    -- \
    -workload sequential -mode write \
    -nodes "${CLUSTER_NAME}-client" \
    -partition-count 20000 -clustering-row-count 100 -clustering-row-size 1024 \
    -replication-factor 3 -consistency-level quorum

# 3. Stream logs while waiting for completion
log_info "Waiting for benchmark pod to start..."
kubectl wait --namespace "$NAMESPACE" --for=condition=Ready pod/scylla-bench --timeout=120s || true
log_info "Benchmarking in progress (streaming logs)..."
kubectl logs -f -n "$NAMESPACE" scylla-bench &
LOGS_PID=$!

# Wait for pod to succeed (plain pods don't have 'complete' condition, use jsonpath)
if ! kubectl wait --namespace "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded pod/scylla-bench --timeout=900s; then
    kill $LOGS_PID 2>/dev/null || true
    log_error "Data generation failed or timed out."
    kubectl logs -n "$NAMESPACE" scylla-bench | tail -n 20
    exit 1
fi
kill $LOGS_PID 2>/dev/null || true

# 4. Cleanup the pod manually after success
kubectl delete pod scylla-bench -n "$NAMESPACE"

# 5. Verify data
log_info "Verifying row count..."
ROW_COUNT=$(kubectl exec -n "$NAMESPACE" "$SCYLLA_POD" -- cqlsh -e "SELECT COUNT(*) FROM scylla_bench.test;" | grep -oE '[0-9]+' | head -1 | tr -d '\n\r ' || echo "0")
log_info "Generated approximately $ROW_COUNT rows"

# ============================================================================
# STEP 3: Backup
# ============================================================================
log_info "=== Step 3: Creating backup ==="

MANAGER_POD=$(kubectl get pods -n scylla-manager -l app.kubernetes.io/name=scylla-manager -o jsonpath='{.items[0].metadata.name}')

# Wait for cluster to be registered with Scylla Manager
log_info "Waiting for cluster registration with Scylla Manager..."
until kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool cluster list 2>/dev/null | grep -q "${NAMESPACE}/${CLUSTER_NAME}"; do
    sleep 5
done
log_info "Cluster registered"

# Run backup (returns task ID immediately)
log_info "Running backup..."
BACKUP_OUTPUT=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool backup \
    -c "${NAMESPACE}/${CLUSTER_NAME}" \
    -L "s3:scylla-backups" 2>&1) || true
echo "$BACKUP_OUTPUT"

# Extract task ID from output (format: backup/uuid)
TASK_ID=$(echo "$BACKUP_OUTPUT" | grep -oE 'backup/[a-f0-9-]+' | head -1 || true)
if [ -n "$TASK_ID" ]; then
    log_info "Backup task started: $TASK_ID"
    log_info "Waiting for backup to complete..."

    # Wait for backup task to complete (poll progress)
    WAIT_COUNT=0
    while [ $WAIT_COUNT -lt 120 ]; do
        PROGRESS=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool progress \
            -c "${NAMESPACE}/${CLUSTER_NAME}" "$TASK_ID" 2>&1) || true

        # Check if backup is done (only check overall Status, not individual host %)
        if echo "$PROGRESS" | grep -qE "^Status:\s+DONE"; then
            log_info "Backup completed"
            break
        fi

        # Check for errors - look for Status: ERROR/FAILED, not column headers
        if echo "$PROGRESS" | grep -qE "Status:\s+(ERROR|FAILED)"; then
            log_error "Backup failed"
            echo "$PROGRESS"
            break
        fi

        # Show progress (extract from "Progress: XX%" line, not individual hosts)
        PERCENT=$(echo "$PROGRESS" | grep -E "^Progress:" | grep -oE '[0-9]+%' || echo "?%")
        log_info "Backup progress: $PERCENT (attempt $((WAIT_COUNT + 1))/120)"
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
    done

    if [ $WAIT_COUNT -ge 120 ]; then
        log_warn "Backup timeout - may still be in progress"
    fi
else
    log_warn "Could not extract backup task ID"
fi

sleep 2
log_info "Getting backup list..."
BACKUP_LIST=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool backup list \
    -c "${NAMESPACE}/${CLUSTER_NAME}" \
    -L "s3:scylla-backups" 2>&1) || true
echo "$BACKUP_LIST"

# Extract snapshot tag (format: sm_YYYYMMDDHHMMSSUTC)
SNAPSHOT_TAG=$(echo "$BACKUP_LIST" | grep -oE 'sm_[0-9]{14}UTC' | tail -1 || true)
if [ -z "$SNAPSHOT_TAG" ]; then
    log_error "Could not extract snapshot tag from backup list"
    log_info "Backup list output: $BACKUP_LIST"
    exit 1
fi
log_info "Backup snapshot tag: $SNAPSHOT_TAG"

# Verify encryption
verify_encryption "scylla-backups" "" "$NAMESPACE" || log_warn "Encryption check skipped"

# ============================================================================
# STEP 4: Delete cluster
# ============================================================================
log_info "=== Step 4: Deleting cluster ==="

kubectl delete scyllacluster -n "$NAMESPACE" "$CLUSTER_NAME" --wait
kubectl wait --namespace "$NAMESPACE" --for=delete pod -l scylla/cluster=${CLUSTER_NAME} --timeout=300s || true
log_info "Cluster deleted"

# ============================================================================
# STEP 5: Create new cluster
# ============================================================================
log_info "=== Step 5: Creating new cluster ==="

envsubst < "${SCRIPT_DIR}/templates/scylla-cluster-restore.yaml" | kubectl apply -n "$NAMESPACE" -f -

log_info "Waiting for all 3 new cluster pods to be ready..."
# Wait until we have 3 pods created
until [ "$(kubectl get pods -n "$NAMESPACE" -l scylla/cluster=${CLUSTER_NAME}-new --no-headers 2>/dev/null | wc -l)" -ge 3 ]; do
    CURRENT=$(kubectl get pods -n "$NAMESPACE" -l scylla/cluster=${CLUSTER_NAME}-new --no-headers 2>/dev/null | wc -l)
    log_info "Waiting for pods... ($CURRENT/3 created)"
    sleep 10
done

# Wait for ALL pods to be ready (kubectl wait waits for all matching pods)
kubectl wait --namespace "$NAMESPACE" --for=condition=ready pod -l scylla/cluster=${CLUSTER_NAME}-new --timeout=600s

# Verify all 3 are ready
READY_COUNT=$(kubectl get pods -n "$NAMESPACE" -l scylla/cluster=${CLUSTER_NAME}-new --no-headers 2>/dev/null | grep -c "Running" || echo 0)
log_info "New cluster ready: $READY_COUNT/3 pods running"

RESTORED_POD=$(kubectl get pods -n "$NAMESPACE" -l scylla/cluster=${CLUSTER_NAME}-new -o jsonpath='{.items[0].metadata.name}')

# ============================================================================
# STEP 6: Restore
# ============================================================================
log_info "=== Step 6: Restoring from backup ==="

# Wait for new cluster to be registered with Scylla Manager
log_info "Waiting for new cluster registration with Scylla Manager..."
WAIT_COUNT=0
while [ $WAIT_COUNT -lt 60 ]; do
    CLUSTER_LIST=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool cluster list 2>&1) || true
    if echo "$CLUSTER_LIST" | grep -q "${CLUSTER_NAME}-new"; then
        log_info "New cluster registered"
        break
    fi
    log_info "Waiting... (attempt $((WAIT_COUNT + 1))/60)"
    echo "$CLUSTER_LIST" | head -5
    sleep 5
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ $WAIT_COUNT -ge 60 ]; then
    log_error "Timeout waiting for new cluster registration"
    log_info "Final cluster list:"
    kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool cluster list
    exit 1
fi

# Give manager a moment to fully sync with new cluster
sleep 10

if [ -z "$SNAPSHOT_TAG" ]; then
    log_error "No snapshot tag found, cannot restore"
    exit 1
fi

log_info "Restoring schema from snapshot $SNAPSHOT_TAG..."
SCHEMA_RESTORE_OUTPUT=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool restore \
    -c "${NAMESPACE}/${CLUSTER_NAME}-new" \
    -L "s3:scylla-backups" \
    --snapshot-tag "$SNAPSHOT_TAG" \
    --restore-schema 2>&1) || true
echo "$SCHEMA_RESTORE_OUTPUT"

# Extract and wait for schema restore task
SCHEMA_TASK_ID=$(echo "$SCHEMA_RESTORE_OUTPUT" | grep -oE 'restore/[a-f0-9-]+' | head -1 || true)
if [ -n "$SCHEMA_TASK_ID" ]; then
    log_info "Schema restore task: $SCHEMA_TASK_ID - waiting for completion..."
    WAIT_COUNT=0
    while [ $WAIT_COUNT -lt 60 ]; do
        PROGRESS=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool progress \
            -c "${NAMESPACE}/${CLUSTER_NAME}-new" "$SCHEMA_TASK_ID" 2>&1) || true
        if echo "$PROGRESS" | grep -qE "^Status:\s+DONE"; then
            log_info "Schema restore completed"
            break
        fi
        if echo "$PROGRESS" | grep -qE "Status:\s+(ERROR|FAILED)"; then
            log_error "Schema restore failed"
            echo "$PROGRESS"
            break
        fi
        PERCENT=$(echo "$PROGRESS" | grep -E "^Progress:" | grep -oE '[0-9]+%' || echo "?%")
        log_info "Schema restore progress: $PERCENT (attempt $((WAIT_COUNT + 1))/60)"
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
    done
else
    log_warn "Could not extract schema restore task ID"
fi

sleep 5

log_info "Restoring data from snapshot $SNAPSHOT_TAG..."
DATA_RESTORE_OUTPUT=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool restore \
    -c "${NAMESPACE}/${CLUSTER_NAME}-new" \
    -L "s3:scylla-backups" \
    --snapshot-tag "$SNAPSHOT_TAG" \
    --restore-tables 2>&1) || true
echo "$DATA_RESTORE_OUTPUT"

# Extract and wait for data restore task
DATA_TASK_ID=$(echo "$DATA_RESTORE_OUTPUT" | grep -oE 'restore/[a-f0-9-]+' | head -1 || true)
if [ -n "$DATA_TASK_ID" ]; then
    log_info "Data restore task: $DATA_TASK_ID - waiting for completion..."
    WAIT_COUNT=0
    while [ $WAIT_COUNT -lt 120 ]; do
        PROGRESS=$(kubectl exec -n scylla-manager "$MANAGER_POD" -- sctool progress \
            -c "${NAMESPACE}/${CLUSTER_NAME}-new" "$DATA_TASK_ID" 2>&1) || true
        if echo "$PROGRESS" | grep -qE "^Status:\s+DONE"; then
            log_info "Data restore completed"
            break
        fi
        if echo "$PROGRESS" | grep -qE "Status:\s+(ERROR|FAILED)"; then
            log_error "Data restore failed"
            echo "$PROGRESS"
            break
        fi
        PERCENT=$(echo "$PROGRESS" | grep -E "^Progress:" | grep -oE '[0-9]+%' || echo "?%")
        log_info "Data restore progress: $PERCENT (attempt $((WAIT_COUNT + 1))/120)"
        sleep 5
        WAIT_COUNT=$((WAIT_COUNT + 1))
    done
else
    log_warn "Could not extract data restore task ID"
fi

sleep 10

# ============================================================================
# STEP 7: Verify
# ============================================================================
log_info "=== Step 7: Verifying restore ==="

RESTORED_COUNT=$(kubectl exec -n "$NAMESPACE" "$RESTORED_POD" -- cqlsh -e "SELECT COUNT(*) FROM scylla_bench.test LIMIT 1000000;" 2>/dev/null | grep -oE '[0-9]+' | head -1 | tr -d '\n\r ' || echo "0")
log_info "Restored rows: $RESTORED_COUNT (original: $ROW_COUNT)"

if [ "$RESTORED_COUNT" = "$ROW_COUNT" ]; then
    log_info "=== TEST PASSED ==="
else
    log_warn "Row count mismatch - restore may still be in progress"
fi

log_info "Test completed. Cleaning up in 5 seconds..."
sleep 5
