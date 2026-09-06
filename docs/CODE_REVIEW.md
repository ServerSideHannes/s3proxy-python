# S3Proxy Python: correctness, performance, and maintainability review

Reviewed on September 5, 2026. Repository: `s3proxy-python`. Reviewed commit: `27b77cf92b5956c9ae671d581403294205dfed77`.

The working tree was clean when reviewed. No application code was changed. This English report translates the earlier review and expands the proposed fixes and validation criteria.

The largest immediate performance opportunities are removing forced garbage collection from request cleanup, reusing S3 clients and connection pools, and reducing backend round trips. Several correctness issues should be addressed first: multipart operations can report false success, overwrite data using conflicting part numbers, reuse cryptographic nonces, and leave objects unreadable after an overwrite.

## Scope and evidence

The review focuses on the Python backend: GET, HEAD, PUT, multipart operations, metadata persistence, request signatures, and memory management. It is not a complete security audit of the dashboard, Helm chart, or deployment environment.

Reproductions used the repository's mock S3 client, local in-memory upload state, and synthetic credentials. No external storage service was contacted. Findings distinguish reproduced behavior from consequences inferred from code. Performance measurements are local microbenchmarks, not production throughput measurements.

Source references below are relative to the repository root and refer to the reviewed commit. P1 means high priority because of security, data integrity, or false-success behavior; P2 means a correctness or resource-management issue that should follow.

## Priority findings and proposed solutions

### 1. P1 — AES-GCM nonce reuse when a multipart part is replaced

**Source:** `s3proxy/crypto.py:367`, `s3proxy/handlers/multipart/upload_part.py:535`, `s3proxy/state/manager.py:266`.

The nonce is derived only from the upload ID, internal part number, and, for framed encryption, frame index. Re-uploading a client part reuses its internal part numbers. If the content changes, the same DEK and nonce encrypt different plaintexts.

**Reproduced:** upload `AAAA`, then `BBBB`, as part 1 of the same upload. Both encryptions use the same nonce, and XOR of the ciphertext payloads equals XOR of the plaintexts.

Replacing an existing part is valid [S3 UploadPart behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html). Reusing a nonce under the same key violates [AES-GCM's security requirements](https://cryptography.io/en/latest/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM).

**Proposed solution:**

- Give every new encryption a fresh random nonce with an appropriate collision budget, or derive it from a unique encryption-attempt identity in addition to the existing fields.
- Distinguish a new encryption attempt from a network retry. A network retry can safely resend the exact same previously encrypted bytes.
- The nonce is already embedded in the ciphertext. Check legacy readers and any deterministic nonce validation before changing generation behavior.

**Acceptance criteria:** replacing a part with different bytes never reuses a nonce/key pair; transport retries resend identical ciphertext; existing stored objects still decrypt.

### 2. P1 — CompleteMultipartUpload can report success for the wrong upload

**Source:** `s3proxy/handlers/multipart/lifecycle.py:169` and `:274`.

`_try_idempotent_complete_response()` checks that the existing object's size matches its existing metadata sidecar. It does not establish that either belongs to the requested `upload_id`. This check happens before reading the requested upload state or validating the client's part list.

**Reproduced:** after creating a multipart object, submit Complete with `uploadId=never-created` and invalid XML. The handler returns HTTP 200. Consequently, a new upload targeting an existing key can appear complete while the previous object remains in place.

**Proposed solution:** persist a completion record tied to the exact upload ID and committed object generation, including the resulting client ETag. Accept an idempotent retry only when that record proves the requested upload completed. Do not infer completion from object size.

**Acceptance criteria:** an unknown upload ID does not succeed because an older object exists; a retry of the actual completed upload returns its recorded result; a new upload to the same key follows the normal completion path.

### 3. P1 — Replacing a multipart object with a small PUT leaves stale metadata

**Source:** `s3proxy/handlers/objects/put.py:174`, `s3proxy/handlers/objects/get.py:67`.

A buffered PUT replaces the object but leaves its old multipart sidecar. GET and HEAD prioritize that sidecar over the new object's encryption metadata.

**Reproduced:** write `old-content` through streaming PUT, then overwrite it with `new` through a small signed PUT. HEAD still reports 11 bytes instead of 3, and GET fails using the old multipart metadata against the new ciphertext.

**Proposed solution:** identify the storage format and generation from the current object's metadata. Only load a sidecar belonging to that generation. Clean up obsolete sidecars separately. Simply deleting a shared sidecar after every PUT is insufficient because concurrent writers can delete each other's metadata.

**Acceptance criteria:** multipart-to-buffered and buffered-to-multipart overwrites return the new bytes and size; concurrent overwrites never combine one generation's ciphertext with another's metadata. Coordinate this work with finding 7.

### 4. P1 — Small signed PUT requests accept a modified request body

**Source:** `s3proxy/request_handler.py:278`, `s3proxy/client/verifier.py:376`, `s3proxy/handlers/objects/put.py:133`.

Signature verification uses the supplied `x-amz-content-sha256` value, but the buffered PUT path never compares it with the actual body's hash. The larger streaming path performs a separate check.

**Reproduced:** generate a valid SigV4 signature for `original`, preserve the signature and hash header, and replace the body with `changed!`. Header verification succeeds and the PUT handler returns HTTP 200 for the modified data.

**Proposed solution:** centralize payload validation and invoke it for every signed write path. For buffered PUT, compute the actual SHA-256 and reject a mismatch before writing to S3. Preserve the intentional semantics of `UNSIGNED-PAYLOAD`; validate streaming signature formats through their dedicated verification path.

**Acceptance criteria:** a modified signed body is rejected without replacing an existing object. Test just below, at, and above the buffering threshold. This finding does not imply arbitrary signature forgery; it shows that a valid signature does not bind the buffered body as intended.

### 5. P1 — Multipart parts are published before their hash or signature is accepted

**Source:** `s3proxy/handlers/multipart/upload_part.py:204`, `:471`, and `:603`.

Both the backend upload and `add_part()` occur before late signature validation. A failed check returns an error without restoring the modified part or state.

**Reproduced:** UploadPart rejects a wrong SHA-256, but the rejected body's MD5 is already stored in the upload's part state. The backend write can also replace a previously valid part.

**Proposed solution:** make validation the boundary before publication. For buffered parts, validate before uploading. For streamed parts, use isolated staging or bounded disk spooling so unverified bytes cannot overwrite an accepted part. Publish the client-part mapping only after verification succeeds, and clean up failed attempts.

Moving `add_part()` alone is insufficient: the backend part may already have been overwritten. Staging must also respect S3's part numbering, ordering, and size constraints.

**Acceptance criteria:** a rejected replacement leaves both the previous accepted part state and its backend bytes unchanged. Inject hash failure, signature failure, cancellation, and disconnect during replacement.

### 6. P1 — Switching part-number allocation strategies creates collisions

**Source:** `s3proxy/state/manager.py:251`, `s3proxy/crypto.py:173`.

The upload starts with dense numbering: client part 2 maps to internal part 2. When a client part requires multiple internal parts, the upload switches to sparse numbering without relocating or protecting previous allocations.

**Reproduced:** allocate one internal part for client part 2: internal number 2. Then allocate two internals for client part 1: internal numbers 1–2. Both allocations include internal number 2. Concurrency is not required; out-of-order parts suffice.

**Proposed solution:** maintain an explicit, atomically updated mapping from client part and attempt to backend allocations. Do not change the meaning of existing allocations when workload shape changes. Design final assembly to preserve client-part order and S3's maximum part count; a monotonic allocator by itself does not solve final ordering.

**Acceptance criteria:** property-based or randomized tests cover out-of-order uploads, changing part sizes, replacements, and simultaneous allocations. No live allocations overlap, and completed plaintext is ordered correctly.

### 7. P1 — Ciphertext and required metadata are published separately

**Source:** `s3proxy/handlers/objects/put.py:310`, `s3proxy/state/metadata.py:265`.

Streaming PUT completes the object before saving its sidecar. This path does not first persist the new DEK in durable upload state. If the sidecar write fails or the process dies between these operations, ciphertext is already visible and the information needed to decrypt it can be lost. This failure window is identified from code; a process-crash scenario was not executed.

Separately, `load_multipart_metadata()` interprets all exceptions as missing metadata, including service failures, authorization failures, and corrupt compressed data.

**Reproduced:** two simulated HTTP 503 backend errors result in `None`. Streaming PUT does not mark the main object with encryption metadata, so a subsequent GET can select unencrypted passthrough when the sidecar cannot be loaded. Other multipart formats can select an incorrect decryption path instead.

**Proposed solution:**

- Generate a stable object-generation identity before upload. Store the format, generation, and necessary wrapped-key information durably before publishing ciphertext.
- Bind immutable sidecars to that generation and make readers resolve only the referenced generation.
- Design a recoverable commit sequence, including crash recovery and obsolete-generation cleanup. Reordering two independent writes alone does not make them atomic.
- Return `None` only for confirmed metadata absence. Propagate service, permission, and decoding errors instead of treating them as plaintext-object detection.

**Acceptance criteria:** inject failure at each commit step, then restart. Every visible encrypted generation must remain decryptable or produce an explicit recoverable error; never silently serve ciphertext as plaintext. A transient metadata error must not trigger format fallback.

### 8. P1 — aws-chunked decoding accepts truncated and unverified input

**Source:** `s3proxy/streaming/chunked.py:88`, `s3proxy/handlers/objects/put.py:90`.

The decoder does not require a terminal zero-size chunk. It does not validate `chunk-signature`; the upload path also disables ordinary payload hash checking for `STREAMING-*`. The decoder skips the two trailing bytes after chunk data without verifying that they are CRLF.

**Reproduced:** a complete `abc` chunk with an invalid chunk signature, followed by an incomplete chunk, produces `abc` without a decoder error.

**Proposed solution:** implement an explicit parser state machine with header, payload, CRLF, terminal chunk, and supported trailer states. Require a valid end state at EOF. Validate the signature chain or checksum/trailer requirements for each supported encoding. Explicitly reject signed streaming variants that are not correctly verified.

**Acceptance criteria:** test truncation at every framing boundary, invalid CRLF, invalid signatures, missing terminal chunks, and supported trailer variants. Failed validation must not publish an object or accepted part.

### 9. P2 — ETags differ across PUT, HEAD, GET, and LIST

**Source:** `s3proxy/handlers/objects/misc.py:72`, `s3proxy/handlers/objects/get.py:51`, `s3proxy/state/attr_cache.py:21`.

For multipart objects, HEAD and LIST use MD5 of the plaintext size. GET uses the backend ETag. Streaming PUT returns MD5 of the plaintext content. HEAD evaluates conditional headers before replacing its effective ETag with the synthetic response ETag.

**Reproduced:** HEAD and GET return different ETags for the same multipart object. Different contents of equal length necessarily share the synthetic HEAD/LIST ETag.

**Proposed solution:** persist one client-facing ETag in generation-bound metadata and use it consistently for responses and conditional checks. It must distinguish content or object generations rather than only lengths. Define compatible behavior for existing objects whose metadata lacks the field.

**Acceptance criteria:** PUT/Complete, HEAD, GET, and LIST agree. Conditional requests using the returned ETag behave consistently, including after a same-size content replacement.

### 10. P2 — Nested memory reservations can block their own request

**Source:** `s3proxy/handlers/objects/get.py:154`, `s3proxy/concurrency.py:105`.

GET reserves 8 MiB at admission and can subsequently request additional memory. If that extra request is clamped to the entire budget, admission requires no memory to be reserved — while the same GET still holds its baseline reservation.

**Reproduced at limiter level:** a 64 MiB budget, an 8 MiB baseline reservation, and an additional reservation calculated for a 40 MiB object result in SlowDown with the timeout set to zero. Production can wait through the full backpressure timeout. Large single-envelope objects are a legacy/compatibility case; new buffered PUT objects stay below the streaming threshold. Multiple smaller GETs can also hold baseline reservations while waiting for each other to release memory.

**Proposed solution:** reserve the complete working set atomically once the object format and size are known, or implement a reservation transition that cannot leave multiple waiters holding mutually blocking partial allocations. Do not clamp a whole-buffer requirement while pretending actual memory use fits the budget. Use bounded streaming or spooling where feasible, preserving authenticated-decryption semantics.

**Acceptance criteria:** one large compatible GET and several simultaneous smaller GETs either progress within a bounded budget or fail promptly and predictably. Cancellation releases reservations exactly once.

## Performance improvements

| Priority | Change | Evidence and proposed implementation |
|---|---|---|
| 1 | Remove forced full GC from every reservation release | `s3proxy/concurrency.py:199` runs `gc.collect(0)`, `(1)`, and `(2)` synchronously on the event loop. A 25-iteration local measurement had a median of approximately **30.6 ms**, versus **0.03 ms** with those calls mocked out. Retain normal automatic GC and evaluate whether any exceptional reclamation policy is needed under sustained load. Linux also calls `malloc_trim`, which was not measured on macOS. |
| 2 | Reuse S3 clients and connection pools | `s3proxy/client/s3.py:65` creates a fresh SDK client per context and closes it afterward. A shared Session reuses model loading, but does not preserve these clients' connection pools across their lifetimes. Own a bounded client registry in application lifespan, isolated by credentials and endpoint/configuration. Close clients at shutdown or safe eviction, after active streams finish. |
| 3 | Eliminate redundant metadata requests | A small GET without a sidecar performs HEAD, two sidecar probes, and data GET: **four backend requests**. Multipart performs HEAD, sidecar GET, and a second HEAD before data retrieval. Pass the first HEAD result through the read path; skip sidecar probes only when an unambiguous current-format marker permits it. Preserve safe legacy detection. |
| 4 | Fetch contiguous frames in fewer Range GETs | `s3proxy/handlers/objects/get.py:479` performs one GET per frame, normally 8 MiB. A 1 GiB object with full frames needs approximately 128 data GETs. Fetch a larger contiguous ciphertext range while reading, authenticating, and emitting one frame at a time. Preserve bounded buffering, backpressure, and frame-aligned recovery after network errors. |
| 5 | Reduce duplicate listing metadata work | `s3proxy/handlers/buckets.py:211` already uses bounded parallelism and an attribute cache. A cold listing of 1,000 multipart objects still requires roughly 1,000 HEADs and 1,000 sidecar GETs in addition to LIST. Coalesce concurrent lookups for the same generation and evaluate a generation-keyed metadata cache or index. Increasing concurrency alone increases backend pressure and memory consumption. |

The GC measurement is a microbenchmark of reservation release, not a claim of a similar improvement in end-to-end throughput. Measure changes individually against a test backend using p50/p95/p99 latency, time to first byte, MiB/s, backend requests per operation, event-loop lag, peak RSS, and error rate.

The 8 MiB GET reservation also understates the working set during prefetch: the current plaintext frame can remain alive while the next frame is downloaded and decrypted. Model the whole lifecycle rather than reserving only one frame's nominal size. Do not raise general concurrency before validating that model.

## Reducing complexity

### A shared upload pipeline

Use common stages for PutObject and UploadPart:

`read/decode → hash → encrypt frames → stage → verify → publish`

The operations can share streaming, hashing, encryption, and cleanup while retaining operation-specific publication rules. This directly addresses inconsistent hash checking, buffering assumptions, and failure cleanup. For small buffered payloads, verification can occur before encryption and staging.

### A common object descriptor

Resolve an object into one typed descriptor containing format version, generation identity, plaintext size, client-facing ETag, encryption metadata, and frame/part index. GET, HEAD, LIST, and COPY should consume the same interpretation. This removes duplicated metadata resolution and ETag logic.

### Explicit multipart state transitions

Separate receiving, verified, published, and completed states. Model client part numbers separately from backend part allocations, and make attempt identity explicit. State transitions should specify what is durable and what a retry may safely repeat.

### Centralized resource ownership

Make request/stream lifetime own the memory reservation, backend response body, client reference, and final metrics. Prefer small explicit services over additional cross-dependent mixins. Cleanup should follow resource lifetime, including disconnects and exceptions after response headers have been sent.

### Narrow exception handling

Replace broad `except Exception: return None/pass` where the intended condition is “not found.” Keep authorization errors, unavailable backends, corrupt metadata, and actual absence distinct.

Retain the current frame format during initial optimization. `FRAME_PLAINTEXT_SIZE` is part of existing read compatibility; changing it without a versioned format can make stored objects unreadable.

## Additional observations from static inspection

These observations were not included in the executed fault reproductions:

| Observation | Proposed follow-up |
|---|---|
| `s3proxy/handlers/objects/put.py:57` checks If-None-Match through HEAD followed by PUT. Two writers can both pass, and non-NotFound HEAD exceptions are swallowed. | Use supported atomic backend preconditions at publication. Define consistent precondition behavior across buffered and multipart writes; test concurrent writers. |
| `s3proxy/handlers/objects/put.py:174` writes internal metadata but does not forward user `x-amz-meta-*` fields, whereas multipart initialization does. | Centralize user-metadata extraction and reserved-key handling, then test parity across upload paths. |
| `s3proxy/handlers/objects/get.py:38` exits the client context before an unencrypted StreamingResponse is consumed. The mock client does not close connections. | Verify a large, slow unencrypted download against real aiohttp/aiobotocore. Keep the backend client alive until stream cleanup. |
| `s3proxy/request_handler.py:161` acquires memory outside the main `try/finally`; rejection can leave the in-flight metric incremented. Streaming requests are recorded as complete before their bodies finish. | Include admission failures in cleanup and finalize streaming duration/status metrics at stream completion, with explicit accounting for post-header failures. |

## Suggested implementation sequence

1. Add regression coverage for findings 1–8. Correct nonce generation, upload identity, payload validation, and allocation collisions before expanding concurrency.
2. Design generation-bound metadata and recoverable publication together. This addresses stale sidecars, lost decryption information, incorrect format fallback, and completion identity without independent patches that conflict.
3. Remove forced per-request GC and redundant HEAD calls. Benchmark latency and RSS separately after each change.
4. Introduce lifetime-correct client pooling and contiguous frame reads. Test real backend connections, retries, disconnects, and memory limits.
5. Consolidate the shared pipeline, object descriptor, and memory ownership in small changes protected by compatibility tests.

## Validation performed

- Existing unit suite: **642 passed, 2 warnings in 293.07 seconds**.
- Command: `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/unit -q -p no:cacheprovider --disable-warnings`.
- Lint: `.venv/bin/python -m ruff check s3proxy --no-cache` passed.
- The contents of the two warnings were not reviewed in that run.
- No integration tests against real S3/Redis and no production load tests were run.
- The separate reproductions expose cases missing from the passing unit suite. They are diagnostic reproductions, not yet committed regression tests.

