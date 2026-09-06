# Generation-bound writes and streaming changes

This change implements the correctness findings and performance work from [the code review](CODE_REVIEW.md). It changes the format of newly written objects. Read this document before upgrading a running cluster.

## Publication and retries

New buffered PUTs carry `s3proxy-format=single-v3`. Streaming PUTs, multipart uploads and streaming copies carry `s3proxy-format=multipart-v3` and an immutable generation pointer. Required manifests are stored at `.s3proxy-internal/generations/<generation>.meta` **before** publishing ciphertext. The generation is derived from the initial wrapped random upload key; it remains the same if the first copy selects a source key before any writer starts. Readers resolve the pointer from the current HEAD response. A missing, corrupt or unavailable required manifest is an error, never a signal to return ciphertext as plaintext.

Each UploadPart attempt writes a private staging object at `.s3proxy-internal/attempts/<generation>/<attempt>`. Hash/signature validation and completion of that staging object precede publication in upload state. A rejected replacement cannot modify an accepted attempt. When a state write has an uncertain outcome, its completed staging object is retained because the state write may already have succeeded.

Complete reads the accepted client-part mapping, validates the client's ordered ETag list, and assembles the selected staging objects in client order with server-side copies. State remains available until completion succeeds. The manifest records the originating bucket, key, upload ID and client ETag, so size alone cannot prove that a retry succeeded. Redis completion locks renew their leases; loss of a lease interrupts the operation. In-memory lock entries disappear after the last waiter finishes.

The first writer atomically freezes the upload's DEK. A whole-object UploadPartCopy may select its source DEK at that point and snapshot the ciphertext using a source ETag precondition. Subsequent copies using a different key, partial ranges and ordinary uploads encrypt with the already frozen key. No active writer can change that key. New encryption uses fresh random nonces; transport retries reuse the already sealed bytes. Full CopyObject retains native copying and publishes the corresponding manifest first.

## Compatibility and deployment

- Existing single-seal and framed objects remain readable. The existing 8 MiB encryption frame boundary is unchanged. Legacy multipart ETags retain their historical fallback where no stored client ETag exists.
- Upgrade all readers before allowing v3 writes. Old releases do not understand the new generation pointers. Do not run old and new writers against the same keys. Use a maintenance window to drain existing uploads and switch the fleet together.
- Drain or restart legacy in-flight multipart uploads. UploadPart and UploadPartCopy reject the old active layout. Do not rely on completing legacy active state after the upgrade.
- Use persistent Redis for uploads that must survive process restarts or move between pods. Configure its TTL above the maximum permitted upload duration. Loss of v3 upload state fails closed: accepted attempts are not reconstructed by guessing from backend parts. Committed objects do not depend on Redis.
- Keep the manifest namespace available to the same backend credentials that operate on objects. The existing internal-prefix filter hides it from proxy listings. Backend permissions now also need multipart creation/copy, listing and deletion for the attempt prefix.
- `If-None-Match: *` is passed to the backend publication operation, including CompleteMultipartUpload for streaming PUT. Verify support on the target S3-compatible service. Failed preconditions return 412.
- Supported signed streaming mode is `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`. Its entire signature chain, terminal chunk and decoded length are checked. Unsupported trailer/signature modes are explicitly rejected. Clients using trailer checksums must select a supported encoding until that protocol is implemented.
- Control request bodies are bounded at 8 MiB. Client parts are bounded at 5 GiB; assembled uploads remain subject to backend object-size and 10,000-part limits. Manifest JSON is bounded at 10 MiB and rejected before publication when it exceeds the reader limit.

## Storage cleanup

Successful completion and abort attempt to delete all completed staging objects for that upload generation. Replaced attempts remain immutable until terminal cleanup to avoid racing a concurrent completion snapshot. Configure a backend lifecycle rule for `.s3proxy-internal/attempts/` to expire completed orphan attempts and abort incomplete staging MPUs after a period **longer than the maximum supported upload duration and retry window**. For example, seven days is appropriate only if the deployment prohibits uploads lasting that long. This PR does not install or modify bucket lifecycle policies.

Crashes, cancelled writes and failed cleanup can leave attempts or unpublished manifests. Generation manifests are intentionally retained: native copies and backend object versions may reference them. Do not apply an age-only deletion policy to `.s3proxy-internal/generations/`. Reclaiming those manifests requires checking all retained object versions, copy references and active uploads. Automatic generation garbage collection is outside this change; retaining metadata is the safe default.

## Read path, resources and performance

GET, HEAD, LIST and CopyObject share an object descriptor for plaintext size, ETag and manifest interpretation. GET reuses the initial HEAD. Consecutive frames share backend range requests of up to 64 MiB while buffering and authenticating one frame at a time. Retry resumes at the first unpublished frame. Output is emitted in bounded chunks. Legacy large single seals authenticate to a temporary spool before any plaintext is exposed; configure sufficient local temporary disk for concurrent legacy reads.

The frame reader allocates exact-size buffers instead of repeatedly growing bytearrays. This was necessary to prevent allocator fragmentation observed during repeated real HTTP reads. GC and allocator trimming no longer run synchronously on each memory release. GET reserves 32 MiB for the working set, and copy operations own their reservation instead of nesting it inside another reservation.

An application-owned pool retains up to 32 credential-isolated S3 clients, evicts idle entries, and waits for active leases on shutdown. Response ownership keeps bodies, clients and reservations alive until streaming finishes, including failure before the body starts. Metrics finish with the stream; dashboard metric failures cannot prevent reservation release. Concurrent listing lookups for the same object/ETag coalesce and recheck the existing bounded attribute cache. Cold listings still need metadata reads for previously unseen objects; this is not a bucket-wide metadata index.

Staging adds temporary storage and server-side copy work to ordinary multipart writes. Full compatible copies avoid re-encryption; partial copies and copies after an incompatible key selection use bounded re-encryption. The change prioritizes verified publication over overwriting unverified backend parts. It does not promise that every write workload gets faster.

## Measured results

A local macOS/Python 3.14.7 test compared `d45732d` with this implementation against isolated Docker MinIO. Each process wrote a 32 MiB object, performed one warm-up GET, then ten sequential GETs. A fresh process was used for each version. Requests used the same client configuration and verified every returned byte.

| Metric | Before | After |
| --- | ---: | ---: |
| Median GET latency | 317.12 ms | 289.26 ms |
| Maximum observed latency | 332.85 ms | 331.64 ms |
| Median transfer rate | 100.91 MiB/s | 110.63 MiB/s |
| Highest sampled process RSS | 179.23 MiB | 175.06 MiB |

This is about 8.8% lower median latency in this small local sample, not a production forecast. [Raw results](read-benchmark.json) are included. A separate 50-request run stayed below 176 MiB sampled RSS. Sampling occurred after requests and can miss transient peaks; the governor budget is not a hard process RSS limit. No p99 claim or AWS/Ceph throughput claim is made.

Regression tests separately demonstrate eight consecutive frames using one backend GET, frame-aligned recovery without duplicate plaintext, AWS-published streaming-signature vectors, corrupt legacy seals, cancelled publication, failed completion retry, immutable key selection, coalesced listings and resource cleanup on failed response headers.

## Validation and remaining rollout checks

Local validation: 670 unit tests passed (two upstream deprecation warnings); 27 real-backend compatibility tests passed, plus 11 copy/concurrency tests and 56 mock integration tests. Ruff lint and formatting checks passed. The PR CI remains the source of truth for the broader Linux integration shards.

Run `uv run pytest tests/unit -q`, `uv run ruff check .` and `uv run ruff format --check .`. `tests/integration/test_generation_roundtrip.py` exercises real HTTP and MinIO, including hash tampering below/at/above the buffering threshold, generation overwrite, conditional writes, ListParts, native copying and UploadPartCopy. Set `S3PROXY_TEST_REDIS_URL` to a dedicated test Redis to repeat the scenario with durable state. `S3PROXY_TEST_BACKEND` selects the backend for the shared integration fixtures.

Before production rollout, run the target backend's compatibility suite and workload benchmarks with its real latency, upload sizes, concurrency, lifecycle policy and pod limits. Existing benchmark/compatibility tests for legacy copy internals remain explicitly separate from the public v3 route. No production deployment or bucket policy changes are part of this PR.
