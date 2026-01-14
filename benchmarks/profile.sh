#!/usr/bin/env bash
#
# S3Proxy Profiler
#
# Profiles the S3Proxy during a benchmark run using py-spy.
# Generates flame graphs showing where time is spent.
#
# Usage:
#   ./benchmarks/profile.sh              # Profile with default settings
#   ./benchmarks/profile.sh --duration 30  # Profile for 30 seconds
#
# Requirements:
#   - Docker & Docker Compose
#
# Output:
#   - benchmarks/results/flamegraph.svg  # Interactive flame graph
#   - benchmarks/results/profile.txt     # Top functions by time
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Default configuration
DURATION="20"
CONCURRENT="20"
SIZE="small"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --duration|-d)
            DURATION="$2"
            shift 2
            ;;
        --concurrent|-c)
            CONCURRENT="$2"
            shift 2
            ;;
        --size|-s)
            SIZE="$2"
            shift 2
            ;;
        --help|-h)
            echo "S3Proxy Profiler"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --duration, -d SEC     Profile duration in seconds (default: 20)"
            echo "  --concurrent, -c NUM   Concurrent requests during profile (default: 20)"
            echo "  --size, -s SIZE        Object size: tiny, small, medium, large (default: small)"
            echo "  --help, -h             Show this help"
            echo ""
            echo "Output:"
            echo "  benchmarks/results/flamegraph.svg           - Interactive flame graph"
            echo "  benchmarks/results/profile.speedscope.json  - Detailed profile (speedscope.app)"
            echo "  benchmarks/results/profile.txt              - Text summary"
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

header() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Cleanup
cleanup() {
    log "Stopping services..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
}

main() {
    header "S3PROXY PROFILER"

    echo "Configuration:"
    echo "  Duration:    ${DURATION}s"
    echo "  Concurrency: ${CONCURRENT}"
    echo "  Object Size: ${SIZE}"
    echo ""

    # Create results directory
    mkdir -p "$SCRIPT_DIR/results"

    # Trap cleanup
    trap cleanup EXIT

    log "Starting services..."
    cd "$SCRIPT_DIR"
    docker compose down -v 2>/dev/null || true
    docker compose up -d --build --wait

    # Wait for profiler container to have py-spy and procps installed
    log "Waiting for profiler to be ready (installing py-spy + procps)..."
    until docker exec bench-profiler py-spy --version >/dev/null 2>&1; do
        sleep 2
    done
    log_success "Profiler ready"

    # Wait for benchmark client
    log "Waiting for benchmark client..."
    until docker exec bench-client python -c "import aioboto3" 2>/dev/null; do
        sleep 1
    done
    log_success "Benchmark client ready"

    # Find the Python process PID in the s3proxy container
    log "Finding S3Proxy process..."

    # Wait for pgrep to be available
    until docker exec bench-profiler which pgrep >/dev/null 2>&1; do
        sleep 1
    done

    PID=$(docker exec bench-profiler pgrep -f "uvicorn" 2>/dev/null | head -1 || echo "")
    if [[ -z "$PID" ]]; then
        PID=$(docker exec bench-profiler pgrep -f "python" 2>/dev/null | head -1 || echo "")
    fi

    if [[ -z "$PID" || ! "$PID" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Could not find S3Proxy process${NC}"
        exit 1
    fi
    log_success "Found S3Proxy process: PID $PID"

    header "PROFILING"

    # Start py-spy recording in background
    log "Starting py-spy profiler (recording for ${DURATION}s)..."
    docker exec -d bench-profiler py-spy record \
        --pid "$PID" \
        --duration "$DURATION" \
        --format speedscope \
        --output /results/profile.speedscope.json \
        --subprocesses

    # Also record SVG flame graph
    docker exec -d bench-profiler py-spy record \
        --pid "$PID" \
        --duration "$DURATION" \
        --format flamegraph \
        --output /results/flamegraph.svg \
        --subprocesses

    # Give py-spy a moment to attach
    sleep 2

    # Run benchmark (proxy only, to focus profiling)
    log "Running benchmark to generate load..."
    docker exec bench-client python /bench/bench.py \
        --size "$SIZE" \
        --concurrent "$CONCURRENT" \
        --duration "$((DURATION - 2))" \
        --proxy-only \
        --runs 1

    # Wait for py-spy to finish
    log "Waiting for profiler to complete..."
    sleep 3

    header "PROFILING COMPLETE"

    # Wait a bit more for files to be written
    sleep 2

    # Generate text summary from speedscope JSON
    if [[ -f "$SCRIPT_DIR/results/profile.speedscope.json" ]]; then
        log "Generating text summary..."
        python3 << 'PYEOF' > "$SCRIPT_DIR/results/profile.txt"
import json
from collections import defaultdict

with open('results/profile.speedscope.json') as f:
    data = json.load(f)

frames = data.get('shared', {}).get('frames', [])
profiles = data.get('profiles', [])

# Aggregate time per function
frame_times = defaultdict(float)
total_time = 0

for profile in profiles:
    if profile.get('type') == 'sampled':
        samples = profile.get('samples', [])
        weights = profile.get('weights', [])
        for sample, weight in zip(samples, weights):
            total_time += weight
            for frame_idx in sample:
                if frame_idx < len(frames):
                    name = frames[frame_idx].get('name', f'frame_{frame_idx}')
                    file = frames[frame_idx].get('file', '')
                    short_file = file.split('/')[-1] if file else ''
                    key = f"{name} ({short_file})" if short_file else name
                    frame_times[key] += weight

# Sort by time
sorted_times = sorted(frame_times.items(), key=lambda x: -x[1])

print("=" * 70)
print("  S3PROXY PROFILE SUMMARY")
print("=" * 70)
print()
print("Top 30 functions by CPU time:")
print()
print(f"{'Function':<50} {'Time':>8} {'%':>8}")
print("-" * 70)

for name, time_us in sorted_times[:30]:
    pct = (time_us / total_time * 100) if total_time > 0 else 0
    time_ms = time_us / 1000
    short_name = name[:48] + ".." if len(name) > 50 else name
    print(f"{short_name:<50} {time_ms:>7.1f}ms {pct:>7.1f}%")

print()
print("=" * 70)
print()

# S3Proxy specific breakdown
print("S3Proxy breakdown:")
print()
s3proxy_funcs = [(n, t) for n, t in sorted_times if any(x in n.lower() for x in ['s3proxy', 'crypto', 'encrypt', 'decrypt', 'handler', 'objects.py', 'buckets.py', 'main.py', 's3client'])]
for name, time_us in s3proxy_funcs[:20]:
    pct = (time_us / total_time * 100) if total_time > 0 else 0
    print(f"  {pct:5.1f}%  {name}")
PYEOF
        log_success "Text summary generated"
    fi

    echo ""
    echo "Results saved to:"
    echo "  - ${SCRIPT_DIR}/results/flamegraph.svg"
    echo "    Open in browser for interactive flame graph"
    echo ""
    echo "  - ${SCRIPT_DIR}/results/profile.speedscope.json"
    echo "    Open at https://speedscope.app for detailed analysis"
    echo ""
    echo "  - ${SCRIPT_DIR}/results/profile.txt"
    echo "    Text summary of top functions"
    echo ""

    # Show summary
    if [[ -f "$SCRIPT_DIR/results/profile.txt" ]]; then
        echo "Quick summary:"
        head -40 "$SCRIPT_DIR/results/profile.txt"
    fi
}

main
