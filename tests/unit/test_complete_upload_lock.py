"""CompleteMultipartUpload cross-pod serialization.

Prod incident 2026-07-31: HAProxy routed concurrent CompleteMultipartUpload
requests for the same upload_id to different pods. Each pod deleted/recovered
state independently and both called upstream CompleteMultipartUpload, yielding
"This multipart completion is already in progress" and eventually NoSuchUpload.
"""

from __future__ import annotations

import asyncio
from urllib.parse import urlencode

import pytest

from s3proxy import crypto
from s3proxy.state import (
    CompleteUploadLock,
    MultipartMetadata,
    PartMetadata,
    save_multipart_metadata,
)
from s3proxy.state.metadata import persist_upload_state

_INTERNAL_FOR_CLIENT = {1: 1, 2: 21}


class _FakeURL:
    def __init__(self, path: str, query: str) -> None:
        self.path = path
        self.query = query


class _FakeRequest:
    def __init__(self, path: str, query: str, body: bytes) -> None:
        self.url = _FakeURL(path, query)
        self.headers: dict[str, str] = {}
        self._body = body

    async def body(self) -> bytes:
        return self._body


class _ClientCM:
    def __init__(self, client) -> None:
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc) -> bool:
        return False


def _complete_request(
    bucket: str, key: str, upload_id: str, client_parts: list[int]
) -> _FakeRequest:
    parts_xml = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>&quot;etag-{n}&quot;</ETag></Part>"
        for n in client_parts
    )
    body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode()
    return _FakeRequest(f"/{bucket}/{key}", urlencode({"uploadId": upload_id}), body)


async def _seed_upload(mock_s3, dek: bytes, bucket: str, key: str, chunks: dict[int, bytes]) -> str:
    resp = await mock_s3.create_multipart_upload(bucket, key)
    upload_id = resp["UploadId"]
    for client_part, plaintext in chunks.items():
        internal = _INTERNAL_FOR_CLIENT[client_part]
        nonce = crypto.derive_part_nonce(upload_id, internal)
        ciphertext = crypto.encrypt(plaintext, dek, nonce)
        await mock_s3.upload_part(bucket, key, upload_id, internal, ciphertext)
    return upload_id


class _CountingCompleteClient:
    """Tracks upstream complete calls and optionally blocks the first one."""

    def __init__(self, inner, *, block_first_until: asyncio.Event | None = None) -> None:
        self._inner = inner
        self.complete_calls = 0
        self._block_first_until = block_first_until
        self._first_complete_started = asyncio.Event()

    async def complete_multipart_upload(self, bucket, key, upload_id, parts):
        self.complete_calls += 1
        if self.complete_calls == 1:
            self._first_complete_started.set()
            if self._block_first_until is not None:
                await self._block_first_until.wait()
        return await self._inner.complete_multipart_upload(bucket, key, upload_id, parts)

    def __getattr__(self, name):
        return getattr(self._inner, name)


@pytest.fixture
def complete_upload_lock(mock_redis):
    return CompleteUploadLock(
        redis_client=mock_redis,
        ttl_seconds=30,
        acquire_timeout_seconds=5,
        poll_interval_seconds=0.05,
    )


@pytest.fixture
def handler_with_lock(settings, manager, complete_upload_lock):
    from s3proxy.handlers.multipart import MultipartHandlerMixin

    h = MultipartHandlerMixin(settings, {}, manager, complete_upload_lock=complete_upload_lock)
    return h


@pytest.mark.asyncio
async def test_complete_lock_serializes_concurrent_upstream_calls(
    handler_with_lock, mock_s3, mock_s3_client, settings, credentials
):
    bucket, key = "test-bucket", "backup/large.db"
    kid, kek = settings.keyring.key_for(credentials.access_key)
    dek = crypto.generate_dek()
    chunk1 = b"first-part" * 64
    chunk2 = b"second-part" * 32
    upload_id = await _seed_upload(mock_s3, dek, bucket, key, {1: chunk1, 2: chunk2})
    await persist_upload_state(
        mock_s3_client, bucket, key, upload_id, crypto.wrap_key(dek, kek), kid
    )

    await handler_with_lock.multipart_manager.create_upload(bucket, key, upload_id, dek, kid)
    for client_part, plaintext in ((1, chunk1), (2, chunk2)):
        internal = _INTERNAL_FOR_CLIENT[client_part]
        nonce = crypto.derive_part_nonce(upload_id, internal)
        ciphertext = crypto.encrypt(plaintext, dek, nonce)
        from s3proxy.state import InternalPartMetadata, PartMetadata

        await handler_with_lock.multipart_manager.add_part(
            bucket,
            key,
            upload_id,
            PartMetadata(
                part_number=client_part,
                plaintext_size=len(plaintext),
                ciphertext_size=len(ciphertext),
                etag="etag",
                md5="etag",
                internal_parts=[
                    InternalPartMetadata(
                        internal_part_number=internal,
                        plaintext_size=len(plaintext),
                        ciphertext_size=len(ciphertext),
                        etag="etag",
                    )
                ],
            ),
        )

    release_first = asyncio.Event()
    counting_client = _CountingCompleteClient(mock_s3_client, block_first_until=release_first)
    handler_with_lock._client = lambda creds: _ClientCM(counting_client)

    req = _complete_request(bucket, key, upload_id, [1, 2])

    first = asyncio.create_task(
        handler_with_lock.handle_complete_multipart_upload(req, credentials)
    )
    await counting_client._first_complete_started.wait()

    second = asyncio.create_task(
        handler_with_lock.handle_complete_multipart_upload(req, credentials)
    )
    await asyncio.sleep(0.1)
    assert counting_client.complete_calls == 1

    release_first.set()
    await asyncio.gather(first, second)

    assert counting_client.complete_calls == 1
    assert f"{bucket}/{key}" in mock_s3.objects


@pytest.mark.asyncio
async def test_complete_lock_idempotent_when_peer_already_finished(
    handler_with_lock, mock_s3, mock_s3_client, settings, credentials
):
    bucket, key = "test-bucket", "backup/done.db"
    kid, kek = settings.keyring.key_for(credentials.access_key)
    dek = crypto.generate_dek()
    chunk1 = b"payload-a" * 32
    chunk2 = b"payload-b" * 16
    upload_id = await _seed_upload(mock_s3, dek, bucket, key, {1: chunk1, 2: chunk2})

    ct1 = crypto.encrypt(chunk1, dek, crypto.derive_part_nonce(upload_id, 1))
    ct2 = crypto.encrypt(chunk2, dek, crypto.derive_part_nonce(upload_id, 21))
    mock_s3.objects[f"{bucket}/{key}"] = {
        "Body": ct1 + ct2,
        "Metadata": {},
        "ContentType": "application/octet-stream",
        "ContentLength": len(ct1) + len(ct2),
        "ETag": "done",
        "LastModified": __import__("datetime").datetime.now(__import__("datetime").UTC),
    }

    parts = [
        PartMetadata(
            part_number=1,
            plaintext_size=len(chunk1),
            ciphertext_size=len(ct1),
            etag="e1",
            md5="e1",
        ),
        PartMetadata(
            part_number=2,
            plaintext_size=len(chunk2),
            ciphertext_size=len(ct2),
            etag="e2",
            md5="e2",
        ),
    ]
    await save_multipart_metadata(
        mock_s3_client,
        bucket,
        key,
        MultipartMetadata(
            version=2,
            upload_id=upload_id,
            part_count=2,
            total_plaintext_size=len(chunk1) + len(chunk2),
            parts=parts,
            wrapped_dek=crypto.wrap_key(dek, kek),
            kid=kid,
        ),
    )

    counting_client = _CountingCompleteClient(mock_s3_client)
    handler_with_lock._client = lambda creds: _ClientCM(counting_client)

    resp = await handler_with_lock.handle_complete_multipart_upload(
        _complete_request(bucket, key, upload_id, [1, 2]), credentials
    )

    assert resp.status_code == 200
    assert counting_client.complete_calls == 0


@pytest.mark.asyncio
async def test_complete_lock_memory_backend_serializes():
    lock = CompleteUploadLock(
        redis_client=None,
        acquire_timeout_seconds=2,
        poll_interval_seconds=0.01,
    )
    order: list[str] = []
    gate = asyncio.Event()

    async def worker(name: str) -> None:
        async with lock.hold("b", "k", "upload-1"):
            order.append(f"{name}-start")
            if name == "a":
                gate.set()
                await asyncio.sleep(0.05)
            order.append(f"{name}-end")

    task_a = asyncio.create_task(worker("a"))
    await gate.wait()
    task_b = asyncio.create_task(worker("b"))
    await asyncio.gather(task_a, task_b)

    assert order == ["a-start", "a-end", "b-start", "b-end"]


@pytest.mark.asyncio
async def test_complete_lock_redis_acquire_timeout_raises(mock_redis):
    lock = CompleteUploadLock(
        redis_client=mock_redis,
        ttl_seconds=30,
        acquire_timeout_seconds=0.15,
        poll_interval_seconds=0.05,
    )

    async with lock.hold("bucket", "key", "upload-locked"):
        with pytest.raises(Exception) as exc:
            async with lock.hold("bucket", "key", "upload-locked"):
                pass

    assert getattr(exc.value, "code", None) == "SlowDown"


@pytest.mark.asyncio
async def test_lease_is_renewed_and_registry_released():
    from fakeredis.aioredis import FakeRedis

    redis = FakeRedis()
    lock = CompleteUploadLock(redis_client=redis, ttl_seconds=1)
    async with lock.hold("bucket", "key", "upload"):
        await asyncio.sleep(1.2)
        assert await redis.exists(lock._redis_key("bucket", "key", "upload"))
    assert not await redis.exists(lock._redis_key("bucket", "key", "upload"))
    local = CompleteUploadLock()
    async with local.hold("bucket", "key", "upload"):
        assert len(local._memory_locks) == 1
    assert not local._memory_locks
    await redis.aclose()
