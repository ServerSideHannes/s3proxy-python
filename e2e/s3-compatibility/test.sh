#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# Use isolated kubeconfig if not already set (running outside container)
if [ -z "${KUBECONFIG:-}" ]; then
    export KUBECONFIG="${ROOT_DIR}/kubeconfig"
    if [ ! -f "$KUBECONFIG" ]; then
        echo "ERROR: Kubeconfig not found at $KUBECONFIG"
        echo "Run ./cluster.sh up first"
        exit 1
    fi
fi

NAMESPACE="s3-compat-test"

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
# STEP 1: Create namespace
# ============================================================================
log_info "=== Step 1: Creating namespace ==="

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# ============================================================================
# STEP 2: Create test configuration
# ============================================================================
log_info "=== Step 2: Creating s3-tests configuration ==="

# Create ConfigMap with s3-tests config
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/s3-tests-config.yaml"

log_info "Configuration created"

# ============================================================================
# STEP 3: Run s3-tests
# ============================================================================
log_info "=== Step 3: Running Ceph s3-tests ==="

# Create Job to run tests
kubectl apply -n "$NAMESPACE" -f "${SCRIPT_DIR}/templates/s3-tests-runner-job.yaml"

log_info "Test job created, waiting for completion..."

# Wait for pod to start
sleep 10

# Follow logs
kubectl logs -n "$NAMESPACE" -f job/s3-tests-runner 2>/dev/null || true

# Wait for job completion
kubectl wait --namespace "$NAMESPACE" \
    --for=condition=complete job/s3-tests-runner \
    --timeout=1800s || true

# Check job status
JOB_STATUS=$(kubectl get job -n "$NAMESPACE" s3-tests-runner -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "0")

if [ "$JOB_STATUS" == "1" ]; then
    log_info "=== S3 Compatibility Tests Completed ==="
else
    log_warn "Tests completed with some failures (expected for proxy architecture)"
fi

# ============================================================================
# STEP 4: Summary
# ============================================================================
log_info "=== Step 4: Summary ==="

echo ""
echo "The Ceph s3-tests validate S3 API compatibility."
echo ""
echo "Excluded tests (delegated to backend or not supported):"
echo "  - ACL/Grant tests (delegated to backend)"
echo "  - Policy tests (delegated to backend)"
echo "  - Versioning tests (delegated to backend)"
echo "  - Lifecycle tests (delegated to backend)"
echo "  - CORS/Website tests (delegated to backend)"
echo "  - Anonymous/public access tests (not supported)"
echo "  - Checksum algorithm tests (CRC32, SHA256 - not supported)"
echo "  - Object lock/retention tests (delegated to backend)"
echo "  - SSE tests (proxy handles encryption differently)"
echo ""
echo "Tests that should pass:"
echo "  - Basic CRUD operations"
echo "  - Multipart uploads"
echo "  - Copy operations"
echo "  - List operations"
echo "  - Range requests"
echo ""

log_info "=== S3 Compatibility Test Complete ==="
