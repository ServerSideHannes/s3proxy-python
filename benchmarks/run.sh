#!/usr/bin/env bash
#
# S3Proxy Benchmark Runner
#
# Runs benchmarks comparing direct MinIO access vs S3Proxy (with encryption).
# This shows the performance overhead of the encryption proxy.
#
# Usage:
#   ./benchmarks/run.sh              # Run all benchmarks
#   ./benchmarks/run.sh --quick      # Quick run (10s, fewer concurrent)
#   ./benchmarks/run.sh --size small # Specific object size
#   ./benchmarks/run.sh --help       # Show help
#
# Requirements:
#   - Docker & Docker Compose
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Default configuration
DURATION="30"
CONCURRENT="10"
SIZES="small"
RUNS="3"
QUICK_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick|-q)
            QUICK_MODE=true
            DURATION="10"
            CONCURRENT="5"
            SIZES="small"
            RUNS="1"
            shift
            ;;
        --duration|-d)
            DURATION="$2"
            shift 2
            ;;
        --concurrent|-c)
            CONCURRENT="$2"
            shift 2
            ;;
        --size|-s)
            SIZES="$2"
            shift 2
            ;;
        --runs|-r)
            RUNS="$2"
            shift 2
            ;;
        --help|-h)
            echo "S3Proxy Benchmark Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick, -q            Quick run (10s, 5 concurrent, 1 run)"
            echo "  --duration, -d SEC     Test duration per run in seconds (default: 30)"
            echo "  --concurrent, -c NUM   Concurrent requests (default: 10)"
            echo "  --runs, -r NUM         Number of runs for statistics (default: 3)"
            echo "  --size, -s SIZE        Object sizes: tiny, small, medium, large (default: small)"
            echo "  --help, -h             Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                     # Default: 3 runs, 30s each, 10 concurrent"
            echo "  $0 --quick             # Quick smoke test (1 run, 10s)"
            echo "  $0 --concurrent 50 --duration 60 --runs 5  # High load test"
            echo "  $0 --size large        # Test large objects (10MB)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

header() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Check dependencies
check_deps() {
    log "Checking dependencies..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi

    log_success "Dependencies OK"
}

# Start services
start_services() {
    log "Starting benchmark services..."

    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    # Only start services needed for benchmark (not profiler)
    docker compose up -d --build --wait redis minio s3proxy benchmark

    log_success "Services running"
    echo "  - MinIO (baseline): http://localhost:9000"
    echo "  - S3Proxy:          http://localhost:8080"
}

# Stop services
stop_services() {
    log "Stopping services..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    log_success "Services stopped"
}

# Run benchmark using separate benchmark container (no resource contention)
run_benchmark() {
    local size="$1"

    log "Running benchmark: $size objects, ${CONCURRENT} concurrent, ${DURATION}s x ${RUNS} runs"

    # Run from separate benchmark container (bench-client)
    # This avoids resource contention with the proxy
    docker exec bench-client python /bench/bench.py \
        --size "$size" \
        --concurrent "$CONCURRENT" \
        --duration "$DURATION" \
        --runs "$RUNS"
}

# Main
main() {
    header "S3PROXY BENCHMARK"

    echo "Configuration:"
    echo "  Duration:    ${DURATION}s per run"
    echo "  Runs:        ${RUNS}"
    echo "  Concurrency: ${CONCURRENT}"
    echo "  Object Size: ${SIZES}"
    if [[ "$QUICK_MODE" == "true" ]]; then
        echo -e "  Mode: ${YELLOW}QUICK${NC}"
    fi

    check_deps

    # Trap to ensure cleanup on exit
    trap stop_services EXIT

    start_services

    # Wait for benchmark container to be ready (deps installed)
    log "Waiting for benchmark client to be ready..."
    until docker exec bench-client python -c "import aioboto3" 2>/dev/null; do
        sleep 1
    done
    log_success "Benchmark client ready"

    # Run benchmarks for each size
    for size in $SIZES; do
        header "Testing: $size objects"
        run_benchmark "$size"
    done

    header "BENCHMARK COMPLETE"
}

main
