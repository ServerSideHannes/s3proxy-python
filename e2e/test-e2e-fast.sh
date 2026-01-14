#!/bin/bash
set -e

# E2E test script for s3proxy encryption - FAST VERSION
# Optimizations:
# - Pre-generate files while Docker starts
# - Run independent test groups in parallel
# - Faster random data generation (openssl)
# - QUICK_MODE for CI (smaller files, fewer iterations)
# - Parallel checksum verification
# - Ramdisk support (Linux)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXY_URL="http://localhost:8080"
MINIO_URL="http://localhost:9000"
BUCKET="test-encrypted-files"

# Configuration
MAX_PARALLEL_JOBS=${MAX_PARALLEL_JOBS:-20}
QUICK_MODE=${QUICK_MODE:-false}
SKIP_LARGE_FILES=${SKIP_LARGE_FILES:-false}

# ============================================================================
# UI Configuration
# ============================================================================

# Colors (with fallback for non-color terminals)
if [ -t 1 ] && [ "${NO_COLOR:-}" != "1" ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    DIM='\033[2m'
    NC='\033[0m'  # No Color
else
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' BOLD='' DIM='' NC=''
fi

# Symbols
PASS_SYM="✓"
FAIL_SYM="✗"
SKIP_SYM="○"
ARROW="→"
SPINNER_CHARS="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


# ============================================================================
# UI Helper Functions
# ============================================================================

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_phase() {
    echo ""
    echo -e "${BOLD}${CYAN}┌─────────────────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BOLD}${CYAN}│  $1$(printf '%*s' $((69 - ${#1})) '')│${NC}"
    echo -e "${BOLD}${CYAN}└─────────────────────────────────────────────────────────────────────────┘${NC}"
}

print_step() {
    echo -e "  ${DIM}${ARROW}${NC} $1"
}

print_success() {
    echo -e "  ${GREEN}${PASS_SYM}${NC} $1"
}

print_error() {
    echo -e "  ${RED}${FAIL_SYM}${NC} $1"
}

print_warning() {
    echo -e "  ${YELLOW}!${NC} $1"
}

# Format bytes to human readable
format_bytes() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=1; $bytes/1073741824" | bc)GB"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(echo "scale=1; $bytes/1048576" | bc)MB"
    elif [ "$bytes" -ge 1024 ]; then
        echo "$(echo "scale=1; $bytes/1024" | bc)KB"
    else
        echo "${bytes}B"
    fi
}

# Start a test with progress indicator
# Returns the start time for use with end_test
start_test() {
    local test_num=$1
    local test_name=$2
    echo ""
    echo -e "${CYAN}Test ${test_num}:${NC} ${test_name}"
    date +%s > "/tmp/test_${test_num}_start"
}

# End a test with result
end_test() {
    local test_num=$1
    local result=$2
    local start_time=$(cat "/tmp/test_${test_num}_start" 2>/dev/null || echo "$(date +%s)")
    local elapsed=$(($(date +%s) - start_time))
    rm -f "/tmp/test_${test_num}_start"

    if [ "$result" = "PASS" ]; then
        echo -e "    ${GREEN}${PASS_SYM} PASSED${NC} ${DIM}(${elapsed}s)${NC}"
    elif [ "$result" = "SKIP" ]; then
        echo -e "    ${YELLOW}${SKIP_SYM} SKIPPED${NC}"
    else
        echo -e "    ${RED}${FAIL_SYM} FAILED${NC} ${DIM}(${elapsed}s)${NC}"
    fi
}

# Progress bar for file operations
show_progress() {
    local current=$1
    local total=$2
    local width=40
    local percent=$((current * 100 / total))
    local filled=$((current * width / total))
    local empty=$((width - filled))

    printf "\r  ${DIM}[${NC}"
    printf "%${filled}s" '' | tr ' ' '█'
    printf "%${empty}s" '' | tr ' ' '░'
    printf "${DIM}]${NC} %3d%%" "$percent"
}

# Adjust sizes for quick mode
if [ "$QUICK_MODE" = true ]; then
    echo -e "${YELLOW}${ARROW} QUICK_MODE enabled - using smaller files${NC}"
    NUM_FILES_CONCURRENT=5
    FILE_SIZE_SMALL=1048576      # 1MB instead of 7MB
    FILE_SIZE_MEDIUM=5242880     # 5MB instead of 20MB
    FILE_SIZE_LARGE=52428800     # 50MB instead of 100MB
    FILE_SIZE_HUGE=104857600     # 100MB instead of 1GB
    STRESS_ROUNDS=1
    STRESS_FILES=2
else
    NUM_FILES_CONCURRENT=30
    FILE_SIZE_SMALL=7340032      # 7MB
    FILE_SIZE_MEDIUM=20971520    # 20MB
    FILE_SIZE_LARGE=104857600    # 100MB
    FILE_SIZE_HUGE=1073741824    # 1GB
    STRESS_ROUNDS=3
    STRESS_FILES=5
fi

print_header "S3Proxy E2E Tests (Fast)"
echo -e "  ${DIM}Files: $(format_bytes $FILE_SIZE_SMALL) / $(format_bytes $FILE_SIZE_MEDIUM) / $(format_bytes $FILE_SIZE_LARGE) / $(format_bytes $FILE_SIZE_HUGE)${NC}"

# Check dependencies
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Create temp directories - prefer ramdisk on Linux
if [ -d /dev/shm ] && [ -w /dev/shm ]; then
    TEST_DIR=$(mktemp -d -p /dev/shm s3proxy-test.XXXXXX)
    DOWNLOAD_DIR=$(mktemp -d -p /dev/shm s3proxy-download.XXXXXX)
    echo -e "  ${DIM}Using ramdisk (/dev/shm) for test files${NC}"
else
    TEST_DIR=$(mktemp -d)
    DOWNLOAD_DIR=$(mktemp -d)
fi
MD5_DIR="$TEST_DIR/.md5"
RESULT_DIR="$TEST_DIR/.results"
mkdir -p "$MD5_DIR" "$RESULT_DIR"

echo -e "  ${DIM}Test directory: $TEST_DIR${NC}"

# Cleanup function
cleanup() {
    echo ""
    echo -e "${DIM}Cleaning up...${NC}"
    cd "$SCRIPT_DIR"
    docker-compose -f docker-compose.e2e.yml down -v 2>/dev/null || true
    rm -rf "$TEST_DIR" "$DOWNLOAD_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# ============================================================================
# Helper Functions
# ============================================================================

wait_for_jobs() {
    local max_jobs=${1:-$MAX_PARALLEL_JOBS}
    while [ "$(jobs -rp | wc -l)" -ge "$max_jobs" ]; do
        sleep 0.05
    done
}

# Fast random file generation - openssl is ~10x faster than /dev/urandom
generate_fast() {
    local file="$1" size="$2"
    if command -v openssl &> /dev/null && [ "$size" -le 104857600 ]; then
        # openssl rand is fast for files up to ~100MB
        openssl rand -out "$file" "$size" 2>/dev/null
    else
        # Fall back to dd for very large files
        dd if=/dev/urandom of="$file" bs=1048576 count=$((size/1048576)) 2>/dev/null
    fi
}

generate_file_with_md5() {
    local file="$1" size="$2"
    local md5_file="$MD5_DIR/$(basename "$file").md5"
    generate_fast "$file" "$size"
    md5 -q "$file" > "$md5_file" 2>/dev/null || md5sum "$file" | cut -d' ' -f1 > "$md5_file"
}

get_md5() {
    local file="$1"
    md5 -q "$file" 2>/dev/null || md5sum "$file" | cut -d' ' -f1
}

# Parallel checksum verification
verify_checksums_parallel() {
    local prefix="$1" count="$2" download_prefix="$3"
    local failed=0
    for i in $(seq 1 $count); do
        (
            orig=$(cat "$MD5_DIR/${prefix}-${i}.bin.md5")
            down=$(get_md5 "$DOWNLOAD_DIR/${download_prefix}-${i}.bin")
            if [ "$orig" != "$down" ]; then
                echo "MISMATCH: $i" >&2
                exit 1
            fi
        ) &
        wait_for_jobs
    done
    wait || failed=1
    return $failed
}

# ============================================================================
# Pre-generate test files while Docker starts
# ============================================================================

pregen_test_files() {
    print_step "Pre-generating test files..."
    local start=$(date +%s)

    # Test 1: Unencrypted single-part files
    for i in $(seq 1 $NUM_FILES_CONCURRENT); do
        generate_file_with_md5 "$TEST_DIR/unenc-single-${i}.bin" "$FILE_SIZE_SMALL" &
        wait_for_jobs
    done

    # Test 2: Encrypted multipart files (10 files)
    for i in $(seq 1 10); do
        generate_file_with_md5 "$TEST_DIR/enc-multi-${i}.bin" "$FILE_SIZE_MEDIUM" &
        wait_for_jobs
    done

    # Test 3: Unencrypted multipart files (10 files)
    for i in $(seq 1 10); do
        generate_file_with_md5 "$TEST_DIR/unenc-multi-${i}.bin" "$FILE_SIZE_MEDIUM" &
        wait_for_jobs
    done

    # Test 4: Stress test files
    for i in $(seq 1 $STRESS_FILES); do
        generate_file_with_md5 "$TEST_DIR/stress-source-${i}.bin" "$FILE_SIZE_MEDIUM" &
        wait_for_jobs
    done

    # Test 5: Various sizes
    for size in 100 1024 102400 1048576 5242880 10485760; do
        generate_file_with_md5 "$TEST_DIR/size-${size}.bin" "$size" &
        wait_for_jobs
    done

    # Test 6: Pattern file for encryption verification
    yes "PLAINTEXT_TEST_DATA_1234567890" | head -c 10485760 > "$TEST_DIR/pattern-test.bin" &

    # Test 7: Passthrough files
    for size in 1024 1048576 10485760; do
        generate_file_with_md5 "$TEST_DIR/passthrough-${size}.bin" "$size" &
        wait_for_jobs
    done

    # Test 10: Presigned URL test files
    generate_file_with_md5 "$TEST_DIR/presigned-5mb.bin" 5242880 &
    generate_file_with_md5 "$TEST_DIR/presigned-1mb.bin" 1048576 &
    generate_file_with_md5 "$TEST_DIR/presigned-large.bin" "$FILE_SIZE_LARGE" &

    wait

    # Calculate pattern MD5
    get_md5 "$TEST_DIR/pattern-test.bin" > "$MD5_DIR/pattern-test.bin.md5"

    print_success "Pre-generation completed ${DIM}($(($(date +%s) - start))s)${NC}"
}

# ============================================================================
# Test Functions (can run in parallel where independent)
# ============================================================================

test_1_unenc_single_part() {
    start_test "1" "Concurrent unencrypted single-part downloads"
    local bucket="test-concurrent-unenc-single"
    aws s3 mb s3://$bucket --endpoint-url $MINIO_URL 2>/dev/null || true

    # Upload (files already generated)
    print_step "Uploading ${NUM_FILES_CONCURRENT} files..."
    for i in $(seq 1 $NUM_FILES_CONCURRENT); do
        aws s3 cp "$TEST_DIR/unenc-single-${i}.bin" "s3://$bucket/file-${i}.bin" --endpoint-url $MINIO_URL >/dev/null 2>&1 &
        wait_for_jobs
    done
    wait

    # Download through proxy
    print_step "Downloading through proxy..."
    local start=$(date +%s)
    for i in $(seq 1 $NUM_FILES_CONCURRENT); do
        aws s3 cp "s3://$bucket/file-${i}.bin" "$DOWNLOAD_DIR/unenc-single-download-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
        wait_for_jobs
    done
    wait
    print_step "Downloads completed in $(($(date +%s) - start))s"

    # Verify
    if verify_checksums_parallel "unenc-single" "$NUM_FILES_CONCURRENT" "unenc-single-download"; then
        echo "PASS" > "$RESULT_DIR/test1"
        end_test "1" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test1"
        end_test "1" "FAIL"
    fi
}

test_2_enc_multipart() {
    start_test "2" "Concurrent encrypted multipart downloads"
    local bucket="test-concurrent-enc-multi"
    local count=10
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    # Upload through proxy (encrypted)
    print_step "Uploading ${count} encrypted files..."
    for i in $(seq 1 $count); do
        aws s3 cp "$TEST_DIR/enc-multi-${i}.bin" "s3://$bucket/file-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
        wait_for_jobs 5  # Limit concurrent uploads to avoid overwhelming proxy
    done
    wait

    # Download
    print_step "Downloading through proxy..."
    local start=$(date +%s)
    for i in $(seq 1 $count); do
        aws s3 cp "s3://$bucket/file-${i}.bin" "$DOWNLOAD_DIR/enc-multi-download-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
        wait_for_jobs
    done
    wait
    print_step "Downloads completed in $(($(date +%s) - start))s"

    # Verify
    if verify_checksums_parallel "enc-multi" "$count" "enc-multi-download"; then
        echo "PASS" > "$RESULT_DIR/test2"
        end_test "2" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test2"
        end_test "2" "FAIL"
    fi
}

test_3_unenc_multipart() {
    start_test "3" "Concurrent unencrypted multipart downloads"
    local bucket="test-concurrent-unenc-multi"
    local count=10
    aws s3 mb s3://$bucket --endpoint-url $MINIO_URL 2>/dev/null || true

    # Upload directly to MinIO
    print_step "Uploading ${count} files to MinIO..."
    for i in $(seq 1 $count); do
        aws s3 cp "$TEST_DIR/unenc-multi-${i}.bin" "s3://$bucket/file-${i}.bin" --endpoint-url $MINIO_URL >/dev/null 2>&1 &
        wait_for_jobs
    done
    wait

    # Download through proxy
    print_step "Downloading through proxy..."
    local start=$(date +%s)
    for i in $(seq 1 $count); do
        aws s3 cp "s3://$bucket/file-${i}.bin" "$DOWNLOAD_DIR/unenc-multi-download-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
        wait_for_jobs
    done
    wait
    print_step "Downloads completed in $(($(date +%s) - start))s"

    # Verify
    if verify_checksums_parallel "unenc-multi" "$count" "unenc-multi-download"; then
        echo "PASS" > "$RESULT_DIR/test3"
        end_test "3" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test3"
        end_test "3" "FAIL"
    fi
}

test_4_stress() {
    start_test "4" "Mixed workload stress test"
    local bucket="test-stress-mixed"
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    # Pre-upload files for download tests
    print_step "Preparing source files..."
    for i in $(seq 1 $STRESS_FILES); do
        aws s3 cp "$TEST_DIR/stress-source-${i}.bin" "s3://$bucket/download-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
    done
    wait

    local all_passed=true
    for round in $(seq 1 $STRESS_ROUNDS); do
        print_step "Round ${round}/${STRESS_ROUNDS}: generating files..."
        # Generate upload files for this round
        for i in $(seq 1 $STRESS_FILES); do
            generate_file_with_md5 "$TEST_DIR/stress-upload-${round}-${i}.bin" "$FILE_SIZE_MEDIUM" &
        done
        wait

        print_step "Round ${round}/${STRESS_ROUNDS}: concurrent upload/download..."
        # Simultaneous uploads and downloads
        for i in $(seq 1 $STRESS_FILES); do
            aws s3 cp "$TEST_DIR/stress-upload-${round}-${i}.bin" "s3://$bucket/upload-${round}-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
            aws s3 cp "s3://$bucket/download-${i}.bin" "$DOWNLOAD_DIR/stress-download-${round}-${i}.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1 &
        done
        wait

        # Verify downloads
        for i in $(seq 1 $STRESS_FILES); do
            orig=$(cat "$MD5_DIR/stress-source-${i}.bin.md5")
            down=$(get_md5 "$DOWNLOAD_DIR/stress-download-${round}-${i}.bin")
            if [ "$orig" != "$down" ]; then
                all_passed=false
            fi
        done

        # Cleanup round
        rm -f "$TEST_DIR"/stress-upload-${round}-*.bin "$DOWNLOAD_DIR"/stress-download-${round}-*.bin
    done

    if [ "$all_passed" = true ]; then
        echo "PASS" > "$RESULT_DIR/test4"
        end_test "4" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test4"
        end_test "4" "FAIL"
    fi
}

test_5_various_sizes() {
    start_test "5" "Various file sizes"
    aws s3 mb s3://$BUCKET --endpoint-url $PROXY_URL 2>/dev/null || true

    local all_passed=true
    local sizes=(100 1024 102400 1048576 5242880 10485760)
    local total=${#sizes[@]}
    local current=0

    for size in "${sizes[@]}"; do
        current=$((current + 1))
        print_step "Testing $(format_bytes $size) [${current}/${total}]..."
        aws s3 cp "$TEST_DIR/size-${size}.bin" "s3://$BUCKET/test-${size}.bin" --endpoint-url $PROXY_URL >/dev/null
        aws s3 cp "s3://$BUCKET/test-${size}.bin" "$DOWNLOAD_DIR/size-${size}.bin" --endpoint-url $PROXY_URL >/dev/null

        orig=$(cat "$MD5_DIR/size-${size}.bin.md5")
        down=$(get_md5 "$DOWNLOAD_DIR/size-${size}.bin")
        if [ "$orig" != "$down" ]; then
            print_error "Checksum mismatch at $(format_bytes $size)"
            all_passed=false
        fi
    done

    if [ "$all_passed" = true ]; then
        echo "PASS" > "$RESULT_DIR/test5"
        end_test "5" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test5"
        end_test "5" "FAIL"
    fi
}

test_6_encryption_at_rest() {
    start_test "6" "Encryption at rest verification"

    print_step "Uploading pattern file through proxy..."
    aws s3 cp "$TEST_DIR/pattern-test.bin" "s3://$BUCKET/pattern-test.bin" --endpoint-url $PROXY_URL >/dev/null

    # Download from MinIO directly (encrypted)
    print_step "Fetching raw data from MinIO..."
    aws s3 cp "s3://$BUCKET/pattern-test.bin" "$DOWNLOAD_DIR/pattern-encrypted.bin" --endpoint-url $MINIO_URL 2>/dev/null || true

    local passed=true
    if [ -f "$DOWNLOAD_DIR/pattern-encrypted.bin" ]; then
        # Check plaintext not in encrypted file
        print_step "Verifying plaintext is not visible..."
        if grep -q "PLAINTEXT_TEST_DATA" "$DOWNLOAD_DIR/pattern-encrypted.bin" 2>/dev/null; then
            print_error "Plaintext found in encrypted storage!"
            passed=false
        fi

        # Verify decryption works
        print_step "Verifying decryption through proxy..."
        aws s3 cp "s3://$BUCKET/pattern-test.bin" "$DOWNLOAD_DIR/pattern-decrypted.bin" --endpoint-url $PROXY_URL >/dev/null
        orig=$(cat "$MD5_DIR/pattern-test.bin.md5")
        down=$(get_md5 "$DOWNLOAD_DIR/pattern-decrypted.bin")
        if [ "$orig" != "$down" ]; then
            print_error "Decryption checksum mismatch!"
            passed=false
        fi
    fi

    if [ "$passed" = true ]; then
        echo "PASS" > "$RESULT_DIR/test6"
        end_test "6" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test6"
        end_test "6" "FAIL"
    fi
}

test_7_passthrough() {
    start_test "7" "Unencrypted passthrough"
    local bucket="test-passthrough"
    aws s3 mb s3://$bucket --endpoint-url $MINIO_URL 2>/dev/null || true

    local all_passed=true
    local sizes=(1024 1048576 10485760)
    local total=${#sizes[@]}
    local current=0

    for size in "${sizes[@]}"; do
        current=$((current + 1))
        print_step "Testing $(format_bytes $size) passthrough [${current}/${total}]..."
        # Upload to MinIO directly
        aws s3 cp "$TEST_DIR/passthrough-${size}.bin" "s3://$bucket/passthrough-${size}.bin" --endpoint-url $MINIO_URL >/dev/null
        # Download through proxy
        aws s3 cp "s3://$bucket/passthrough-${size}.bin" "$DOWNLOAD_DIR/passthrough-${size}.bin" --endpoint-url $PROXY_URL >/dev/null

        orig=$(cat "$MD5_DIR/passthrough-${size}.bin.md5")
        down=$(get_md5 "$DOWNLOAD_DIR/passthrough-${size}.bin")
        if [ "$orig" != "$down" ]; then
            print_error "Passthrough failed for $(format_bytes $size)"
            all_passed=false
        fi
    done

    if [ "$all_passed" = true ]; then
        echo "PASS" > "$RESULT_DIR/test7"
        end_test "7" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test7"
        end_test "7" "FAIL"
    fi
}

test_8_list_filtering() {
    start_test "8" "ListObjects filtering"
    local bucket="test-meta-filtering"
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    # Upload files
    print_step "Uploading test files..."
    echo "test" > "$TEST_DIR/manifest.txt"
    aws s3 cp "$TEST_DIR/manifest.txt" "s3://$bucket/backups/manifest" --endpoint-url $PROXY_URL >/dev/null
    aws s3 cp "$TEST_DIR/size-1048576.bin" "s3://$bucket/backups/large.bin" --endpoint-url $PROXY_URL >/dev/null

    # Inject .s3proxy-meta directly
    print_step "Injecting metadata file directly to MinIO..."
    echo "meta" > "$TEST_DIR/meta.txt"
    aws s3 cp "$TEST_DIR/meta.txt" "s3://$bucket/backups/injected.s3proxy-meta" --endpoint-url $MINIO_URL >/dev/null

    # Check filtering
    print_step "Verifying metadata files are hidden..."
    local proxy_listing=$(aws s3 ls "s3://$bucket/backups/" --endpoint-url $PROXY_URL 2>/dev/null || echo "")
    if echo "$proxy_listing" | grep -q "\.s3proxy-meta"; then
        print_error ".s3proxy-meta visible through proxy!"
        echo "FAIL" > "$RESULT_DIR/test8"
        end_test "8" "FAIL"
    else
        echo "PASS" > "$RESULT_DIR/test8"
        end_test "8" "PASS"
    fi
}

test_10_presigned_urls() {
    start_test "10" "Presigned URL operations"
    local bucket="test-presigned"
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    local all_passed=true

    # Presigned GET (encrypted)
    print_step "Testing presigned GET (encrypted)..."
    aws s3 cp "$TEST_DIR/presigned-5mb.bin" "s3://$bucket/encrypted.bin" --endpoint-url $PROXY_URL >/dev/null
    local url=$(aws s3 presign "s3://$bucket/encrypted.bin" --endpoint-url $PROXY_URL --expires-in 300)
    if curl -sf "$url" -o "$DOWNLOAD_DIR/presigned-enc.bin"; then
        orig=$(cat "$MD5_DIR/presigned-5mb.bin.md5")
        down=$(get_md5 "$DOWNLOAD_DIR/presigned-enc.bin")
        [ "$orig" != "$down" ] && all_passed=false
    else
        print_error "Presigned encrypted download failed"
        all_passed=false
    fi

    # Presigned GET (unencrypted passthrough)
    print_step "Testing presigned GET (passthrough)..."
    aws s3 cp "$TEST_DIR/presigned-5mb.bin" "s3://$bucket/unencrypted.bin" --endpoint-url $MINIO_URL >/dev/null
    url=$(aws s3 presign "s3://$bucket/unencrypted.bin" --endpoint-url $PROXY_URL --expires-in 300)
    if curl -sf "$url" -o "$DOWNLOAD_DIR/presigned-unenc.bin"; then
        down=$(get_md5 "$DOWNLOAD_DIR/presigned-unenc.bin")
        [ "$orig" != "$down" ] && all_passed=false
    else
        print_error "Presigned passthrough download failed"
        all_passed=false
    fi

    # Presigned GET (large encrypted)
    print_step "Testing presigned GET (large file)..."
    aws s3 cp "$TEST_DIR/presigned-large.bin" "s3://$bucket/large.bin" --endpoint-url $PROXY_URL >/dev/null
    url=$(aws s3 presign "s3://$bucket/large.bin" --endpoint-url $PROXY_URL --expires-in 600)
    if curl -sf "$url" -o "$DOWNLOAD_DIR/presigned-large.bin"; then
        orig=$(cat "$MD5_DIR/presigned-large.bin.md5")
        down=$(get_md5 "$DOWNLOAD_DIR/presigned-large.bin")
        [ "$orig" != "$down" ] && all_passed=false
    else
        print_error "Presigned large file download failed"
        all_passed=false
    fi

    if [ "$all_passed" = true ]; then
        echo "PASS" > "$RESULT_DIR/test10"
        end_test "10" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test10"
        end_test "10" "FAIL"
    fi
}

test_12_large_files() {
    if [ "$SKIP_LARGE_FILES" = true ]; then
        start_test "12" "Large file transfer"
        echo "SKIP" > "$RESULT_DIR/test12"
        end_test "12" "SKIP"
        return
    fi

    start_test "12" "Large file transfer ($(format_bytes $FILE_SIZE_HUGE))"
    local bucket="test-large-files"
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    # Generate large file
    local large_file="$TEST_DIR/large-test.bin"
    print_step "Generating $(format_bytes $FILE_SIZE_HUGE) file..."
    generate_fast "$large_file" "$FILE_SIZE_HUGE"
    local orig=$(get_md5 "$large_file")

    # Upload
    print_step "Uploading..."
    local start=$(date +%s)
    aws s3 cp "$large_file" "s3://$bucket/large.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1
    print_step "Upload completed in $(($(date +%s) - start))s"

    # Download
    print_step "Downloading..."
    start=$(date +%s)
    aws s3 cp "s3://$bucket/large.bin" "$DOWNLOAD_DIR/large.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1
    print_step "Download completed in $(($(date +%s) - start))s"

    local down=$(get_md5 "$DOWNLOAD_DIR/large.bin")
    if [ "$orig" = "$down" ]; then
        echo "PASS" > "$RESULT_DIR/test12"
        end_test "12" "PASS"
    else
        echo "FAIL" > "$RESULT_DIR/test12"
        end_test "12" "FAIL"
    fi

    rm -f "$large_file" "$DOWNLOAD_DIR/large.bin"
}

test_13_streaming_put() {
    if [ "$SKIP_LARGE_FILES" = true ]; then
        start_test "13" "Streaming PUT (UNSIGNED-PAYLOAD)"
        echo "SKIP" > "$RESULT_DIR/test13"
        end_test "13" "SKIP"
        return
    fi

    start_test "13" "Streaming PUT (UNSIGNED-PAYLOAD)"
    local bucket="test-streaming-put"
    aws s3 mb s3://$bucket --endpoint-url $PROXY_URL 2>/dev/null || true

    local streaming_file="$TEST_DIR/streaming-test.bin"
    print_step "Generating $(format_bytes $FILE_SIZE_HUGE) file..."
    generate_fast "$streaming_file" "$FILE_SIZE_HUGE"
    local orig=$(get_md5 "$streaming_file")

    # Find Python
    local PYTHON_CMD="python3"
    [ -f ".venv/bin/python" ] && PYTHON_CMD=".venv/bin/python"

    # Upload with UNSIGNED-PAYLOAD
    print_step "Uploading with UNSIGNED-PAYLOAD..."
    local result=$($PYTHON_CMD -c "
import boto3
from botocore.config import Config
s3 = boto3.client('s3', endpoint_url='$PROXY_URL', aws_access_key_id='$AWS_ACCESS_KEY_ID',
    aws_secret_access_key='$AWS_SECRET_ACCESS_KEY', region_name='us-east-1',
    config=Config(signature_version='s3v4', s3={'payload_signing_enabled': False}))
with open('$streaming_file', 'rb') as f:
    s3.put_object(Bucket='$bucket', Key='streaming.bin', Body=f)
print('OK')
" 2>&1)

    if [ "$result" = "OK" ]; then
        print_step "Verifying download..."
        aws s3 cp "s3://$bucket/streaming.bin" "$DOWNLOAD_DIR/streaming.bin" --endpoint-url $PROXY_URL >/dev/null 2>&1
        local down=$(get_md5 "$DOWNLOAD_DIR/streaming.bin")
        if [ "$orig" = "$down" ]; then
            echo "PASS" > "$RESULT_DIR/test13"
            end_test "13" "PASS"
        else
            print_error "Checksum mismatch"
            echo "FAIL" > "$RESULT_DIR/test13"
            end_test "13" "FAIL"
        fi
    else
        print_error "Upload failed"
        echo "FAIL" > "$RESULT_DIR/test13"
        end_test "13" "FAIL"
    fi

    rm -f "$streaming_file" "$DOWNLOAD_DIR/streaming.bin"
}

# ============================================================================
# Main Execution
# ============================================================================

print_phase "Initialization"

# Stop existing services
print_step "Stopping existing services..."
cd "$SCRIPT_DIR"
docker-compose -f docker-compose.e2e.yml down -v 2>/dev/null || true

# Start file pre-generation AND docker build in parallel
print_step "Starting parallel initialization..."
(
    docker-compose -f docker-compose.e2e.yml build s3proxy >/dev/null 2>&1
    docker-compose -f docker-compose.e2e.yml up -d
) &
DOCKER_PID=$!

pregen_test_files &
PREGEN_PID=$!

# Wait for pre-generation (usually faster)
wait $PREGEN_PID
print_success "File pre-generation complete"

# Wait for Docker
wait $DOCKER_PID
print_success "Docker services started"

# Wait for s3proxy to be ready
print_step "Waiting for s3proxy..."
max_retries=30
retry=0
while [ $retry -lt $max_retries ]; do
    if curl -sf http://localhost:8080/readyz > /dev/null 2>&1; then
        print_success "s3proxy ready"
        break
    fi
    retry=$((retry + 1))
    [ $retry -eq $max_retries ] && { print_error "s3proxy not ready after ${max_retries}s"; exit 1; }
    sleep 1
done

# Configure AWS CLI
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="$PROXY_URL"
export AWS_RETRY_MODE="adaptive"
export AWS_MAX_ATTEMPTS="10"

mkdir -p ~/.aws
cat > ~/.aws/config <<EOF
[default]
cli_read_timeout = 600
cli_connect_timeout = 60
s3 =
    max_concurrent_requests = 5
    multipart_threshold = 8MB
    multipart_chunksize = 8MB
EOF

# Create main bucket
aws s3 mb s3://$BUCKET --endpoint-url $PROXY_URL 2>/dev/null || true

total_start=$(date +%s)

# Run independent test groups in parallel (Tests 1, 2, 3)
print_phase "Phase 1: Concurrent Download Tests (parallel)"
test_1_unenc_single_part &
test_2_enc_multipart &
test_3_unenc_multipart &
wait

# Run remaining tests in parallel (each uses separate buckets/keys)
print_phase "Phase 2: Functional Tests (parallel)"
test_4_stress &
test_5_various_sizes &
test_6_encryption_at_rest &
test_7_passthrough &
test_8_list_filtering &
test_10_presigned_urls &
wait

# Large file tests (parallel but resource-intensive)
print_phase "Phase 3: Large File Tests (parallel)"
test_12_large_files &
test_13_streaming_put &
wait

# Final health check
echo ""
if curl -sf http://localhost:8080/healthz > /dev/null 2>&1; then
    print_success "s3proxy healthy after all tests"
else
    print_error "s3proxy not responding!"
    exit 1
fi

# Summary
total_time=$(($(date +%s) - total_start))
echo ""
echo -e "${BOLD}${BLUE}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${BLUE}║                            TEST SUMMARY                                  ║${NC}"
echo -e "${BOLD}${BLUE}╠══════════════════════════════════════════════════════════════════════════╣${NC}"

all_passed=true
pass_count=0
fail_count=0
skip_count=0

for result_file in $(ls "$RESULT_DIR"/* 2>/dev/null | sort -V); do
    test_name=$(basename "$result_file")
    result=$(cat "$result_file")
    # Extract test number for description
    test_num=${test_name#test}

    if [ "$result" = "PASS" ]; then
        echo -e "${BOLD}${BLUE}║${NC}  ${GREEN}${PASS_SYM}${NC} Test ${test_num}: ${GREEN}PASSED${NC}$(printf '%*s' $((52 - ${#test_num})) '')${BOLD}${BLUE}║${NC}"
        pass_count=$((pass_count + 1))
    elif [ "$result" = "SKIP" ]; then
        echo -e "${BOLD}${BLUE}║${NC}  ${YELLOW}${SKIP_SYM}${NC} Test ${test_num}: ${YELLOW}SKIPPED${NC}$(printf '%*s' $((51 - ${#test_num})) '')${BOLD}${BLUE}║${NC}"
        skip_count=$((skip_count + 1))
    else
        echo -e "${BOLD}${BLUE}║${NC}  ${RED}${FAIL_SYM}${NC} Test ${test_num}: ${RED}FAILED${NC}$(printf '%*s' $((52 - ${#test_num})) '')${BOLD}${BLUE}║${NC}"
        fail_count=$((fail_count + 1))
        all_passed=false
    fi
done

echo -e "${BOLD}${BLUE}╠══════════════════════════════════════════════════════════════════════════╣${NC}"
echo -e "${BOLD}${BLUE}║${NC}  ${DIM}Total: ${pass_count} passed, ${fail_count} failed, ${skip_count} skipped${NC}$(printf '%*s' $((40 - ${#pass_count} - ${#fail_count} - ${#skip_count})) '')${DIM}(${total_time}s)${NC}  ${BOLD}${BLUE}║${NC}"
echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════════════════════════════════════╝${NC}"

echo ""
if [ "$all_passed" = true ]; then
    echo -e "${BOLD}${GREEN}✓ All tests passed!${NC}"
    exit 0
else
    echo -e "${BOLD}${RED}✗ Some tests failed!${NC}"
    exit 1
fi
